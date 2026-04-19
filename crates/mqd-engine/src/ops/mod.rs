pub mod proximity;
pub mod speed;

pub use proximity::{proximity_pairs, ProximityQuery, ProximityResult};
pub use speed::{speed_over_ground, SpeedQuery, SpeedResult};
