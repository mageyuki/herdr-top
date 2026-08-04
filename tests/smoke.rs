// Resolving this import at compile time proves the library exposes the module.
#[allow(unused_imports)]
use herdr_top::session_key as _;

#[test]
fn lib_target_exposes_session_key() {
    // Importing the module above proves the library target exposes the scaffolded tree.
}
