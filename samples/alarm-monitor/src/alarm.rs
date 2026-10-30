//! The alarm state machine and how the module reads its twin: pure functions, unit tested.
use superset_module_sdk::{InputTelemetry, ModuleTwin, Telemetry};

/// The twin field for the upper limit, and its default.
pub const HIGH_LIMIT: &str = "highLimit";
pub const DEFAULT_HIGH_LIMIT: f64 = 30.0;
/// The twin field for the lower limit, and its default.
pub const LOW_LIMIT: &str = "lowLimit";
pub const DEFAULT_LOW_LIMIT: f64 = 10.0;
/// The name of the linked variable the module watches (sent by the smoother sample).
pub const TEMP_AVG: &str = "temp_avg";
/// The names of the module's own variables.
pub const ALARM: &str = "alarm";
pub const ALARM_COUNT: &str = "alarm_count";

/// The alarm band: an alarm is on above `high` or below `low`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Limits {
    pub high: f64,
    pub low: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            high: DEFAULT_HIGH_LIMIT,
            low: DEFAULT_LOW_LIMIT,
        }
    }
}

impl Limits {
    /// The limits the twin sets, and the fields that fell back to their default (missing, not a
    /// number, or a low limit not below the high one, which resets both).
    pub fn from_twin(twin: &ModuleTwin) -> (Self, Vec<&'static str>) {
        let mut defaulted = Vec::new();
        let mut read =
            |field: &'static str, default: f64| match twin.get(field).and_then(|v| v.as_f64()) {
                Some(value) if value.is_finite() => value,
                _ => {
                    defaulted.push(field);
                    default
                }
            };
        let high = read(HIGH_LIMIT, DEFAULT_HIGH_LIMIT);
        let low = read(LOW_LIMIT, DEFAULT_LOW_LIMIT);
        if low >= high {
            for field in [HIGH_LIMIT, LOW_LIMIT] {
                if !defaulted.contains(&field) {
                    defaulted.push(field);
                }
            }
            return (Self::default(), defaulted);
        }
        (Self { high, low }, defaulted)
    }

    /// True when the value is outside the band.
    pub fn alarms(&self, value: f64) -> bool {
        value > self.high || value < self.low
    }
}

/// The module's own variable ids, found by their `name` in the twin.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Owned {
    pub alarm: Option<String>,
    pub alarm_count: Option<String>,
}

impl Owned {
    pub fn from_twin(twin: &ModuleTwin) -> Self {
        Self {
            alarm: variable_named(twin, ALARM),
            alarm_count: variable_named(twin, ALARM_COUNT),
        }
    }
}

/// The id of the first variable (in id order) whose `name` is `name`.
pub fn variable_named(twin: &ModuleTwin, name: &str) -> Option<String> {
    twin.variable_ids()
        .find(|id| twin.declared_name(id) == Some(name))
        .map(str::to_owned)
}

/// The smoothed temperature in a linked message: a `temp_avg` reading with a number value.
pub fn temp_avg(input: &InputTelemetry) -> Option<f64> {
    (input.name.as_deref() == Some(TEMP_AVG))
        .then(|| input.value.as_f64())
        .flatten()
}

/// The alarm and its count of 0 -> 1 transitions since start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AlarmState {
    pub active: bool,
    pub count: u64,
}

impl AlarmState {
    /// Takes one smoothed temperature; true when the alarm was just raised (a 0 -> 1 transition).
    pub fn observe(&mut self, value: f64, limits: &Limits) -> bool {
        let active = limits.alarms(value);
        let raised = active && !self.active;
        self.active = active;
        if raised {
            self.count += 1;
        }
        raised
    }

    /// One reading per owned variable: `alarm` (0 or 1) and `alarm_count`.
    pub fn readings(&self, owned: &Owned) -> Vec<Telemetry> {
        let mut readings = Vec::new();
        if let Some(id) = &owned.alarm {
            readings.push(Telemetry::new(id.as_str(), u8::from(self.active)));
        }
        if let Some(id) = &owned.alarm_count {
            readings.push(Telemetry::new(id.as_str(), self.count));
        }
        readings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use superset_module_sdk::twin::parse_twin_answer;

    fn twin(fields: Value) -> ModuleTwin {
        let answer = json!({"modules": {"alarm-monitor": fields}});
        parse_twin_answer(answer.to_string().as_bytes(), "alarm-monitor").unwrap()
    }

    fn input(name: Option<&str>, value: Value) -> InputTelemetry {
        InputTelemetry {
            topic: "output/telemetry/t/n/smoother/v".into(),
            variable_id: "variable-1".into(),
            value,
            name: name.map(str::to_owned),
            timestamp: None,
        }
    }

    #[test]
    fn limits_come_from_the_twin() {
        let (limits, defaulted) =
            Limits::from_twin(&twin(json!({"highLimit": 25, "lowLimit": 5.5})));
        assert_eq!(
            limits,
            Limits {
                high: 25.0,
                low: 5.5
            }
        );
        assert!(defaulted.is_empty());
    }

    #[test]
    fn missing_or_invalid_limits_fall_back_and_are_named() {
        let (limits, defaulted) = Limits::from_twin(&twin(json!({})));
        assert_eq!(limits, Limits::default());
        assert_eq!(defaulted, [HIGH_LIMIT, LOW_LIMIT]);

        let (limits, defaulted) =
            Limits::from_twin(&twin(json!({"highLimit": "hot", "lowLimit": 0})));
        assert_eq!(
            limits,
            Limits {
                high: DEFAULT_HIGH_LIMIT,
                low: 0.0
            }
        );
        assert_eq!(defaulted, [HIGH_LIMIT]);

        // A band that is empty or upside down resets both limits.
        for (high, low) in [(10, 10), (5, 20)] {
            let (limits, defaulted) =
                Limits::from_twin(&twin(json!({"highLimit": high, "lowLimit": low})));
            assert_eq!(limits, Limits::default());
            assert_eq!(defaulted, [HIGH_LIMIT, LOW_LIMIT]);
        }
    }

    #[test]
    fn alarms_outside_the_band_only() {
        let limits = Limits::default();
        assert!(!limits.alarms(20.0));
        assert!(!limits.alarms(30.0), "the limits themselves are inside");
        assert!(!limits.alarms(10.0));
        assert!(limits.alarms(30.5));
        assert!(limits.alarms(9.9));
    }

    #[test]
    fn counts_rising_edges_only() {
        let limits = Limits::default();
        let mut state = AlarmState::default();
        let steps = [
            (20.0, false, false, 0),
            (35.0, true, true, 1),
            (36.0, false, true, 1), // still on: not a new alarm
            (20.0, false, false, 1),
            (5.0, true, true, 2),   // low side counts too
            (40.0, false, true, 2), // low to high without passing through the band
            (15.0, false, false, 2),
        ];
        for (value, raised, active, count) in steps {
            assert_eq!(state.observe(value, &limits), raised, "{value}");
            assert_eq!(state, AlarmState { active, count }, "{value}");
        }
    }

    #[test]
    fn finds_own_variables_by_name() {
        let twin = twin(json!({"variables": {
            "variable-b": {"name": "alarm_count"},
            "variable-a": {"name": "alarm"},
            "variable-c": {"name": "alarm"},
            "variable-d": {}
        }}));
        let owned = Owned::from_twin(&twin);
        assert_eq!(owned.alarm.as_deref(), Some("variable-a"));
        assert_eq!(owned.alarm_count.as_deref(), Some("variable-b"));
        assert_eq!(Owned::from_twin(&self::twin(json!({}))), Owned::default());
    }

    #[test]
    fn readings_cover_the_owned_variables() {
        let owned = Owned {
            alarm: Some("variable-a".into()),
            alarm_count: Some("variable-b".into()),
        };
        let state = AlarmState {
            active: true,
            count: 3,
        };
        let readings = state.readings(&owned);
        assert_eq!(readings.len(), 2);
        assert_eq!(
            (readings[0].variable_id.as_str(), &readings[0].value),
            ("variable-a", &json!(1))
        );
        assert_eq!(
            (readings[1].variable_id.as_str(), &readings[1].value),
            ("variable-b", &json!(3))
        );
        let only_alarm = Owned {
            alarm: Some("variable-a".into()),
            alarm_count: None,
        };
        assert_eq!(
            AlarmState::default().readings(&only_alarm)[0].value,
            json!(0)
        );
        assert!(state.readings(&Owned::default()).is_empty());
    }

    #[test]
    fn watches_temp_avg_numbers_only() {
        assert_eq!(temp_avg(&input(Some("temp_avg"), json!(21.5))), Some(21.5));
        assert_eq!(temp_avg(&input(Some("temp_avg"), json!(22))), Some(22.0));
        assert_eq!(temp_avg(&input(Some("temp_f"), json!(70.7))), None);
        assert_eq!(temp_avg(&input(None, json!(21.5))), None);
        assert_eq!(temp_avg(&input(Some("temp_avg"), json!("hot"))), None);
        assert_eq!(temp_avg(&input(Some("temp_avg"), json!([1, 2]))), None);
    }
}
