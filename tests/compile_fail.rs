#[test]
fn typed_boundaries_and_evidence_capabilities_do_not_cross() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
