#![allow(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "workload-harness"))]
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};

/// Supplies monotonic timestamps to the runtime performance tracker.
pub trait PerformanceClock: Send + Sync {
    fn monotonic_now(&self) -> Duration;
}

/// Process-relative monotonic clock used by ordinary production runtimes.
#[derive(Debug)]
pub struct SystemPerformanceClock {
    origin: Instant,
}

impl SystemPerformanceClock {
    /// Starts a process-relative monotonic clock at the current instant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemPerformanceClock {
    fn default() -> Self {
        Self::new()
    }
}

impl PerformanceClock for SystemPerformanceClock {
    fn monotonic_now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// Linux absolute monotonic clock used by authoritative workload evidence.
#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[derive(Clone, Copy, Debug, Default)]
#[doc(hidden)]
pub struct AbsoluteMonotonicPerformanceClock;

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
impl PerformanceClock for AbsoluteMonotonicPerformanceClock {
    fn monotonic_now(&self) -> Duration {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) };
        if result != 0 {
            panic!(
                "clock_gettime(CLOCK_MONOTONIC) failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let seconds = u64::try_from(value.tv_sec)
            .unwrap_or_else(|_| panic!("CLOCK_MONOTONIC returned negative seconds"));
        let nanoseconds = u32::try_from(value.tv_nsec)
            .unwrap_or_else(|_| panic!("CLOCK_MONOTONIC returned negative nanoseconds"));
        assert!(
            nanoseconds < 1_000_000_000,
            "CLOCK_MONOTONIC returned out-of-range nanoseconds"
        );
        Duration::new(seconds, nanoseconds)
    }
}

/// Manually controlled monotonic clock for deterministic tests and workloads.
#[cfg(any(test, feature = "workload-harness"))]
#[derive(Debug)]
#[doc(hidden)]
pub struct TestPerformanceClock {
    nanoseconds: AtomicU64,
}

#[cfg(any(test, feature = "workload-harness"))]
impl TestPerformanceClock {
    /// Creates a manual clock at `now` without sleeping.
    #[must_use]
    pub fn new(now: Duration) -> Self {
        Self {
            nanoseconds: AtomicU64::new(duration_nanoseconds(now)),
        }
    }

    /// Sets the manual clock to `now` without sleeping.
    pub fn set(&self, now: Duration) {
        self.nanoseconds
            .store(duration_nanoseconds(now), Ordering::Relaxed);
    }

    /// Advances the manual clock by `amount` without sleeping.
    pub fn advance(&self, amount: Duration) {
        let amount = duration_nanoseconds(amount);
        self.nanoseconds
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(
                    current
                        .checked_add(amount)
                        .expect("test performance clock nanosecond overflow"),
                )
            })
            .expect("test performance clock update is infallible");
    }
}

#[cfg(any(test, feature = "workload-harness"))]
impl PerformanceClock for TestPerformanceClock {
    fn monotonic_now(&self) -> Duration {
        Duration::from_nanos(self.nanoseconds.load(Ordering::Relaxed))
    }
}

#[cfg(any(test, feature = "workload-harness"))]
fn duration_nanoseconds(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).expect("test performance clock nanosecond overflow")
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PerformanceDegradationReason {
    LivePanes,
    DefaultVisibleTaskRuns,
    DependencyEdges,
    EventsOneSecond,
    EventsTenSeconds,
    EventsSixtySeconds,
    EventLag,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PerformanceSnapshot {
    pub event_lag: Duration,
    pub pending_events: usize,
    pub admission_high_water: u64,
    pub completion_high_water: u64,
    pub events_one_second: usize,
    pub events_ten_seconds: usize,
    pub events_sixty_seconds: usize,
    pub live_panes: usize,
    pub default_visible_task_runs: usize,
    pub dependency_edges: usize,
    pub execution_edges: usize,
    pub reasons: BTreeSet<PerformanceDegradationReason>,
}

struct PerformanceState {
    admission_high_water: u64,
    completion_high_water: u64,
    pending: BTreeMap<u64, Duration>,
    admission_times: VecDeque<Duration>,
    lag_breach_high_water: Option<u64>,
}

impl PerformanceState {
    fn new() -> Self {
        Self {
            admission_high_water: 0,
            completion_high_water: 0,
            pending: BTreeMap::new(),
            admission_times: VecDeque::new(),
            lag_breach_high_water: None,
        }
    }

    fn admit(&mut self, admitted_at: Duration) -> AdmissionStamp {
        let sequence = self
            .admission_high_water
            .checked_add(1)
            .expect("performance admission sequence exhausted");
        self.admission_high_water = sequence;
        let replaced = self.pending.insert(sequence, admitted_at);
        assert!(replaced.is_none(), "performance sequence must be unique");
        self.admission_times.push_back(admitted_at);
        AdmissionStamp {
            sequence,
            admitted_at,
        }
    }

    fn complete(&mut self, sequence: u64) {
        if self.pending.remove(&sequence).is_some() {
            self.completion_high_water = self.completion_high_water.max(sequence);
        }
    }

    fn discard_expired_admission_times(&mut self, now: Duration) {
        let Some(boundary) = now.checked_sub(Duration::from_secs(60)) else {
            return;
        };
        while self
            .admission_times
            .front()
            .is_some_and(|admitted_at| *admitted_at <= boundary)
        {
            self.admission_times.pop_front();
        }
    }

    fn count_window(&self, now: Duration, width: Duration) -> usize {
        match now.checked_sub(width) {
            Some(boundary) => self
                .admission_times
                .iter()
                .filter(|admitted_at| **admitted_at > boundary && **admitted_at <= now)
                .count(),
            None => self
                .admission_times
                .iter()
                .filter(|admitted_at| **admitted_at <= now)
                .count(),
        }
    }

    fn event_lag(&self, now: Duration) -> Duration {
        self.pending
            .first_key_value()
            .map_or(Duration::ZERO, |(_, admitted_at)| {
                now.saturating_sub(*admitted_at)
            })
    }

    fn update_lag_generation(&mut self, event_lag: Duration) {
        if self.lag_breach_high_water.is_some_and(|high_water| {
            self.pending
                .first_key_value()
                .is_none_or(|(sequence, _)| *sequence > high_water)
        }) {
            self.lag_breach_high_water = None;
        }
        if self.lag_breach_high_water.is_none()
            && !self.pending.is_empty()
            && event_lag > Duration::from_secs(1)
        {
            self.lag_breach_high_water = Some(self.admission_high_water);
        }
    }
}

struct SharedPerformance {
    clock: Arc<dyn PerformanceClock>,
    state: Mutex<PerformanceState>,
}

#[derive(Clone)]
pub struct PerformanceIngress {
    shared: Arc<SharedPerformance>,
}

pub struct Admission {
    shared: Arc<SharedPerformance>,
    stamp: AdmissionStamp,
    completed: bool,
}

impl Admission {
    pub fn complete(mut self) {
        self.complete_once();
    }

    fn stamp(&self) -> AdmissionStamp {
        self.stamp
    }

    fn complete_once(&mut self) {
        if self.completed {
            return;
        }
        self.shared
            .state
            .lock()
            .expect("performance state mutex poisoned")
            .complete(self.stamp.sequence);
        self.completed = true;
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.complete_once();
    }
}

impl PerformanceIngress {
    #[must_use]
    pub fn admit(&self) -> Admission {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("performance state mutex poisoned");
        let admitted_at = self.shared.clock.monotonic_now();
        let stamp = state.admit(admitted_at);
        drop(state);
        Admission {
            shared: self.shared.clone(),
            stamp,
            completed: false,
        }
    }
}

pub struct PerformanceSampler {
    shared: Arc<SharedPerformance>,
    last_sampled_at: Option<Duration>,
}

pub fn performance_tracker(
    clock: Arc<dyn PerformanceClock>,
) -> (PerformanceIngress, PerformanceSampler) {
    let shared = Arc::new(SharedPerformance {
        clock,
        state: Mutex::new(PerformanceState::new()),
    });
    (
        PerformanceIngress {
            shared: shared.clone(),
        },
        PerformanceSampler {
            shared,
            last_sampled_at: None,
        },
    )
}

impl PerformanceSampler {
    pub fn sample(
        &mut self,
        model: &crate::model::DomainModel,
        operator: &crate::activity::OperatorSnapshot,
        now_ms: i64,
    ) -> PerformanceSnapshot {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("performance state mutex poisoned");
        let now = self.shared.clock.monotonic_now();
        state.discard_expired_admission_times(now);
        let event_lag = state.event_lag(now);
        state.update_lag_generation(event_lag);

        let events_one_second = state.count_window(now, Duration::from_secs(1));
        let events_ten_seconds = state.count_window(now, Duration::from_secs(10));
        let events_sixty_seconds = state.count_window(now, Duration::from_secs(60));
        let live_panes = model.panes().count();
        let default_visible_task_runs =
            crate::activity::default_visible_task_run_count(model, operator, now_ms);
        let dependency_edges = model.dependency_edges().count();
        let execution_edges = model.execution_edges().count();

        let mut reasons = BTreeSet::new();
        if live_panes > 50 {
            reasons.insert(PerformanceDegradationReason::LivePanes);
        }
        if default_visible_task_runs > 200 {
            reasons.insert(PerformanceDegradationReason::DefaultVisibleTaskRuns);
        }
        if dependency_edges > 1_000 {
            reasons.insert(PerformanceDegradationReason::DependencyEdges);
        }
        if events_one_second > 100 {
            reasons.insert(PerformanceDegradationReason::EventsOneSecond);
        }
        if events_ten_seconds > 1_000 {
            reasons.insert(PerformanceDegradationReason::EventsTenSeconds);
        }
        if events_sixty_seconds > 1_200 {
            reasons.insert(PerformanceDegradationReason::EventsSixtySeconds);
        }
        if state.lag_breach_high_water.is_some() {
            reasons.insert(PerformanceDegradationReason::EventLag);
        }

        let snapshot = PerformanceSnapshot {
            event_lag,
            pending_events: state.pending.len(),
            admission_high_water: state.admission_high_water,
            completion_high_water: state.completion_high_water,
            events_one_second,
            events_ten_seconds,
            events_sixty_seconds,
            live_panes,
            default_visible_task_runs,
            dependency_edges,
            execution_edges,
            reasons,
        };
        self.last_sampled_at = Some(now);
        snapshot
    }

    #[cfg(any(test, feature = "workload-harness"))]
    #[doc(hidden)]
    pub fn workload_sampled_at(&self) -> Duration {
        self.last_sampled_at
            .expect("performance snapshot has not been sampled")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionStamp {
    pub sequence: u64,
    pub admitted_at: Duration,
}

pub(crate) type AdmissionObserver<T> = Arc<dyn Fn(AdmissionStamp, &T) + Send + Sync>;

pub struct Admitted<T> {
    value: T,
    admission: Admission,
}

impl<T> Admitted<T> {
    #[cfg(test)]
    pub(crate) fn stamp(&self) -> AdmissionStamp {
        self.admission.stamp()
    }

    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    pub fn workload_stamp(&self) -> (u64, Duration) {
        let stamp = self.admission.stamp();
        (stamp.sequence, stamp.admitted_at)
    }

    pub fn into_parts(self) -> (T, Admission) {
        (self.value, self.admission)
    }
}

pub struct AdmittingSender<T> {
    sender: mpsc::Sender<Admitted<T>>,
    ingress: PerformanceIngress,
    observer: Option<AdmissionObserver<T>>,
}

impl<T> Clone for AdmittingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            ingress: self.ingress.clone(),
            observer: self.observer.clone(),
        }
    }
}

impl<T> AdmittingSender<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(())) => return Err(TrySendError::Full(value)),
            Err(TrySendError::Closed(())) => return Err(TrySendError::Closed(value)),
        };
        let admission = self.ingress.admit();
        if let Some(observer) = &self.observer {
            observer(admission.stamp(), &value);
        }
        permit.send(Admitted { value, admission });
        Ok(())
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let permit = match self.sender.reserve().await {
            Ok(permit) => permit,
            Err(SendError(())) => return Err(SendError(value)),
        };
        let admission = self.ingress.admit();
        if let Some(observer) = &self.observer {
            observer(admission.stamp(), &value);
        }
        permit.send(Admitted { value, admission });
        Ok(())
    }
}

pub fn admitted_channel<T>(
    capacity: usize,
    ingress: PerformanceIngress,
) -> (AdmittingSender<T>, mpsc::Receiver<Admitted<T>>) {
    admitted_channel_observed(capacity, ingress, None)
}

pub(crate) fn admitted_channel_observed<T>(
    capacity: usize,
    ingress: PerformanceIngress,
    observer: Option<AdmissionObserver<T>>,
) -> (AdmittingSender<T>, mpsc::Receiver<Admitted<T>>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        AdmittingSender {
            sender,
            ingress,
            observer,
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::mpsc::error::TrySendError;

    use super::*;
    use crate::activity::OperatorSnapshot;
    use crate::model::{
        DependencyEdge, DisplayOrdinal, DomainModel, ExecutionEdge, Pane, RunId, RunKey, TaskRun,
        TaskState,
    };

    fn empty_inputs() -> (DomainModel, OperatorSnapshot) {
        (
            DomainModel::default(),
            OperatorSnapshot {
                activity: Arc::from(Vec::new()),
                terminal_times: Arc::new(HashMap::new()),
            },
        )
    }

    fn deterministic_run_id(index: usize) -> RunId {
        RunId::parse(&format!("{index:026}")).unwrap()
    }

    fn load_inputs(
        panes: usize,
        visible_runs: usize,
        dependency_edges: usize,
        execution_edges: usize,
    ) -> (DomainModel, OperatorSnapshot) {
        let mut model = DomainModel::default();
        for index in 0..panes {
            model.insert_pane(Pane {
                pane_id: format!("pane-{index:03}"),
                workspace_id: "workspace-000".to_owned(),
                tab_id: "tab-000".to_owned(),
                terminal_id: format!("terminal-{index:03}"),
            });
        }

        let run_ids = (0..visible_runs)
            .map(deterministic_run_id)
            .collect::<Vec<_>>();
        for (index, run_id) in run_ids.iter().copied().enumerate() {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("load-{index:03}")),
                display_ordinal: DisplayOrdinal::new(index as i64 + 1),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            });
        }

        let ordered_pairs = run_ids
            .iter()
            .enumerate()
            .flat_map(|(prerequisite, &left)| {
                run_ids
                    .iter()
                    .skip(prerequisite + 1)
                    .copied()
                    .map(move |right| (left, right))
            });
        for (prerequisite_run_id, dependent_run_id) in ordered_pairs.clone().take(dependency_edges)
        {
            assert!(model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id,
                dependent_run_id,
            }));
        }
        for (parent_run_id, child_run_id) in ordered_pairs.take(execution_edges) {
            assert!(model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            }));
        }
        assert_eq!(model.dependency_edges().count(), dependency_edges);
        assert_eq!(model.execution_edges().count(), execution_edges);

        let operator = OperatorSnapshot {
            activity: Arc::from(Vec::new()),
            terminal_times: Arc::new(HashMap::new()),
        };
        (model, operator)
    }

    #[cfg(all(target_os = "linux", feature = "workload-harness"))]
    fn direct_clock_gettime_monotonic() -> Duration {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) };
        assert_eq!(result, 0);
        Duration::new(
            u64::try_from(value.tv_sec).unwrap(),
            u32::try_from(value.tv_nsec).unwrap(),
        )
    }

    fn admission_observer_collecting<T: 'static>(
        observed: Arc<Mutex<Vec<AdmissionStamp>>>,
    ) -> AdmissionObserver<T> {
        Arc::new(move |stamp, _value| observed.lock().unwrap().push(stamp))
    }

    #[test]
    fn exact_rate_boundaries_pass_and_strictly_greater_values_degrade() {
        for (width, limit, reason) in [
            (
                Duration::from_secs(1),
                100,
                PerformanceDegradationReason::EventsOneSecond,
            ),
            (
                Duration::from_secs(10),
                1_000,
                PerformanceDegradationReason::EventsTenSeconds,
            ),
            (
                Duration::from_secs(60),
                1_200,
                PerformanceDegradationReason::EventsSixtySeconds,
            ),
        ] {
            let clock = Arc::new(TestPerformanceClock::new(width - Duration::from_nanos(1)));
            let (ingress, mut sampler) = performance_tracker(clock);
            let admitted = (0..limit).map(|_| ingress.admit()).collect::<Vec<_>>();
            let (model, operator) = empty_inputs();
            assert!(
                !sampler
                    .sample(&model, &operator, 0)
                    .reasons
                    .contains(&reason)
            );
            let over_limit = ingress.admit();
            assert!(
                sampler
                    .sample(&model, &operator, 0)
                    .reasons
                    .contains(&reason)
            );
            drop(over_limit);
            drop(admitted);
        }
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock.clone());
        ingress.admit().complete();
        clock.set(Duration::from_secs(1));
        let (model, operator) = empty_inputs();
        assert_eq!(sampler.sample(&model, &operator, 0).events_one_second, 0);
    }

    #[test]
    fn lag_reason_latches_until_the_breach_generation_drains() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock.clone());
        let first = ingress.admit();
        clock.advance(Duration::from_millis(500));
        let second = ingress.admit();
        clock.advance(Duration::from_millis(501));
        let (model, operator) = empty_inputs();
        assert!(
            sampler
                .sample(&model, &operator, 0)
                .reasons
                .contains(&PerformanceDegradationReason::EventLag)
        );
        first.complete();
        assert!(
            sampler
                .sample(&model, &operator, 0)
                .reasons
                .contains(&PerformanceDegradationReason::EventLag)
        );
        second.complete();
        assert!(
            !sampler
                .sample(&model, &operator, 0)
                .reasons
                .contains(&PerformanceDegradationReason::EventLag)
        );
    }

    #[test]
    fn rolling_windows_include_origin_without_duration_underflow() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let admission = ingress.admit();
        let (model, operator) = empty_inputs();
        let snapshot = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                snapshot.events_one_second,
                snapshot.events_ten_seconds,
                snapshot.events_sixty_seconds
            ),
            (1, 1, 1)
        );
        admission.complete();
    }

    #[test]
    fn sampler_reports_the_exact_clock_value_used_for_the_snapshot() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::from_nanos(123)));
        let (_ingress, mut sampler) = performance_tracker(clock.clone());
        let (model, operator) = empty_inputs();
        sampler.sample(&model, &operator, 0);
        clock.set(Duration::from_nanos(456));
        assert_eq!(sampler.workload_sampled_at(), Duration::from_nanos(123));
    }

    #[cfg(all(target_os = "linux", feature = "workload-harness"))]
    #[test]
    fn absolute_performance_clock_matches_the_kernel_monotonic_domain() {
        let before = direct_clock_gettime_monotonic();
        let observed = AbsoluteMonotonicPerformanceClock.monotonic_now();
        let after = direct_clock_gettime_monotonic();
        assert!(before <= observed && observed <= after);
    }

    #[test]
    fn model_envelope_boundaries_and_execution_cardinality_are_exact() {
        for (panes, visible_runs, dependency_edges, execution_edges, expected) in [
            (50, 200, 1_000, 1_000, BTreeSet::new()),
            (
                51,
                200,
                1_000,
                1_000,
                BTreeSet::from([PerformanceDegradationReason::LivePanes]),
            ),
            (
                50,
                201,
                1_000,
                1_000,
                BTreeSet::from([PerformanceDegradationReason::DefaultVisibleTaskRuns]),
            ),
            (
                50,
                200,
                1_001,
                1_000,
                BTreeSet::from([PerformanceDegradationReason::DependencyEdges]),
            ),
            (50, 200, 1_000, 5_000, BTreeSet::new()),
        ] {
            let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
            let (_ingress, mut sampler) = performance_tracker(clock);
            let (model, operator) =
                load_inputs(panes, visible_runs, dependency_edges, execution_edges);
            let snapshot = sampler.sample(&model, &operator, 0);
            assert_eq!(
                (
                    snapshot.live_panes,
                    snapshot.default_visible_task_runs,
                    snapshot.dependency_edges,
                    snapshot.execution_edges
                ),
                (panes, visible_runs, dependency_edges, execution_edges)
            );
            assert_eq!(snapshot.reasons, expected);
        }
    }

    #[tokio::test]
    async fn admitted_channel_completes_dropped_and_explicit_admissions_once() {
        struct NonClone(u8);
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let (sender, mut receiver) = admitted_channel(2, ingress.clone());
        let cloned = sender.clone();
        sender.try_send(NonClone(7)).unwrap();
        cloned.try_send(NonClone(8)).unwrap();
        assert!(matches!(
            sender.try_send(NonClone(9)),
            Err(TrySendError::Full(NonClone(9)))
        ));
        let (model, operator) = empty_inputs();
        let before = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                before.pending_events,
                before.admission_high_water,
                before.events_one_second
            ),
            (2, 2, 2)
        );
        drop(receiver.recv().await.unwrap());
        assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 1);
        let (value, admission) = receiver.recv().await.unwrap().into_parts();
        assert_eq!(value.0, 8);
        admission.complete();
        assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 0);

        let (closed_sender, closed_receiver) = admitted_channel(1, ingress);
        drop(closed_receiver);
        assert!(matches!(
            closed_sender.try_send(NonClone(10)),
            Err(TrySendError::Closed(NonClone(10)))
        ));
        let after = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                after.pending_events,
                after.admission_high_water,
                after.events_one_second
            ),
            (0, 2, 2)
        );
        assert_eq!(after.event_lag, Duration::ZERO);
    }

    #[tokio::test]
    async fn observed_admitting_channel_preserves_post_reservation_timestamp() {
        #[allow(dead_code)]
        struct NonClone(u8);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observer = admission_observer_collecting(observed.clone());
        let clock = Arc::new(TestPerformanceClock::new(Duration::from_millis(25)));
        let (ingress, _sampler) = performance_tracker(clock);
        let (sender, mut receiver) = admitted_channel_observed(1, ingress, Some(observer));
        sender.send(NonClone(7)).await.unwrap();
        let admitted = receiver.recv().await.unwrap();
        assert_eq!(observed.lock().unwrap().as_slice(), &[admitted.stamp()]);
    }
}
