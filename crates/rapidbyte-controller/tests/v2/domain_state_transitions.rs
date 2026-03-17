use rapidbyte_controller::domain::lease::Lease;
use rapidbyte_controller::domain::run::{Run, RunId, RunState};

#[test]
fn run_cannot_transition_from_succeeded_to_executing() {
    let mut run = Run::new(RunId::new("r1"));
    run.force_state_for_test(RunState::Succeeded);
    assert!(run.transition(RunState::Executing).is_err());
}

#[test]
fn stale_lease_is_rejected() {
    let lease = Lease::new(10, "agent-a", std::time::SystemTime::now());
    assert!(!lease.matches("agent-a", 9));
}
