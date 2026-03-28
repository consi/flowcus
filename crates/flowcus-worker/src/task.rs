use serde::{Deserialize, Serialize};

/// Priority level for task scheduling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Trait for CPU-bound tasks that can be dispatched to the rayon pool.
pub trait CpuTask: Send + 'static {
    type Output: Send + 'static;

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    fn execute(self) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AddTask(i32, i32);

    impl CpuTask for AddTask {
        type Output = i32;

        fn execute(self) -> Self::Output {
            self.0 + self.1
        }
    }

    #[test]
    fn cpu_task_execution() {
        let task = AddTask(3, 4);
        assert_eq!(task.execute(), 7);
    }

    #[test]
    fn priority_ordering() {
        assert!(TaskPriority::Critical > TaskPriority::High);
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
    }
}
