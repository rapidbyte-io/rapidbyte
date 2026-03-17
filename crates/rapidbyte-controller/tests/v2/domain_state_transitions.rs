use rapidbyte_controller::domain::run::{Run, RunId, RunState};

#[test]
fn run_cannot_transition_from_succeeded_to_executing() {
    let mut run = Run::new(RunId::new("r1"));
    run.force_state_for_test(RunState::Succeeded);
    assert!(run.transition(RunState::Executing).is_err());
}
