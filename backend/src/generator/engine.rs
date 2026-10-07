use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use regex::Regex;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const FIRST_NAMES: &[&str] = &[
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Carlos", "Elena", "Mateo", "Sofia", "Alejandro", "Valentina", "Liam", "Emma"
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
];

const DOMAINS: &[&str] = &[
    "example.com", "mail.com", "company.io", "techcorp.net", "globalnet.org",
    "cloudflow.dev", "streamdata.ai", "fastmetrics.co"
];

const CITIES: &[&str] = &[
    "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto",
    "Singapore", "Amsterdam", "San Francisco", "Austin", "Madrid", "Seoul"
];

const COUNTRY_CODES: &[&str] = &[
    "US", "GB", "DE", "FR", "JP", "CA", "AU", "ES", "NL", "SG", "BR", "MX"
];

pub struct TemplateEngine {
    sequence_counter: AtomicU64,
    re_random_int: Regex,
    re_random_float: Regex,
    re_choose: Regex,
}

impl TemplateEngine {
    pub fn new() -> Self {
        Self {
            sequence_counter: AtomicU64::new(1),
            re_random_int: Regex::new(r"\{\{random_int\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)\}\}").unwrap(),
            re_random_float: Regex::new(r"\{\{random_float\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)(?:\s*,\s*(\d+))?\s*\)\}\}").unwrap(),
            re_choose: Regex::new(r"\{\{choose\(\s*\[(.*?)\]\s*\)\}\}").unwrap(),
        }
    }

    pub fn reset_sequence(&self) {
        self.sequence_counter.store(1, Ordering::Relaxed);
    }

    /// Renders a template string by interpolating mock dynamic variables
    pub fn render(&self, template: &str) -> String {
        let mut rng = rand::thread_rng();
        let mut output = template.to_string();

        // 1. Sequence counter
        if output.contains("{{sequence}}") {
            let seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
            output = output.replace("{{sequence}}", &seq.to_string());
        }

        // 2. UUIDs
        while output.contains("{{uuid}}") || output.contains("{{uuid_v4}}") {
            let u = Uuid::new_v4().to_string();
            if output.contains("{{uuid}}") {
                output = output.replacen("{{uuid}}", &u, 1);
            }
            if output.contains("{{uuid_v4}}") {
                output = output.replacen("{{uuid_v4}}", &u, 1);
            }
        }

        // 3. Timestamps
        let now = Utc::now();
        if output.contains("{{timestamp}}") {
            output = output.replace("{{timestamp}}", &now.timestamp_millis().to_string());
        }
        if output.contains("{{timestamp_s}}") {
            output = output.replace("{{timestamp_s}}", &now.timestamp().to_string());
        }
        if output.contains("{{timestamp_iso}}") {
            output = output.replace("{{timestamp_iso}}", &now.to_rfc3339());
        }

        // 4. Names & Personal info
        while output.contains("{{name}}") {
            let first = FIRST_NAMES.choose(&mut rng).unwrap_or(&"Alex");
            let last = LAST_NAMES.choose(&mut rng).unwrap_or(&"Morgan");
            let full = format!("{} {}", first, last);
            output = output.replacen("{{name}}", &full, 1);
        }
        while output.contains("{{first_name}}") {
            let first = FIRST_NAMES.choose(&mut rng).unwrap_or(&"Alex");
            output = output.replacen("{{first_name}}", first, 1);
        }
        while output.contains("{{last_name}}") {
            let last = LAST_NAMES.choose(&mut rng).unwrap_or(&"Morgan");
            output = output.replacen("{{last_name}}", last, 1);
        }
        while output.contains("{{email}}") {
            let first = FIRST_NAMES.choose(&mut rng).unwrap_or(&"alex").to_lowercase();
            let last = LAST_NAMES.choose(&mut rng).unwrap_or(&"morgan").to_lowercase();
            let domain = DOMAINS.choose(&mut rng).unwrap_or(&"example.com");
            let rand_num: u16 = rng.gen_range(10..999);
            let email = format!("{}.{}{}@{}", first, last, rand_num, domain);
            output = output.replacen("{{email}}", &email, 1);
        }

        // 5. Geographic & Network data
        while output.contains("{{ipv4}}") {
            let ip = format!("{}.{}.{}.{}", rng.gen_range(10..220), rng.gen_range(0..255), rng.gen_range(0..255), rng.gen_range(1..254));
            output = output.replacen("{{ipv4}}", &ip, 1);
        }
        while output.contains("{{country_code}}") {
            let code = COUNTRY_CODES.choose(&mut rng).unwrap_or(&"US");
            output = output.replacen("{{country_code}}", code, 1);
        }
        while output.contains("{{city}}") {
            let city = CITIES.choose(&mut rng).unwrap_or(&"Tokyo");
            output = output.replacen("{{city}}", city, 1);
        }

        // 6. Boolean & Phone & CC
        while output.contains("{{boolean}}") {
            let b: bool = rng.gen();
            output = output.replacen("{{boolean}}", &b.to_string(), 1);
        }
        while output.contains("{{phone}}") {
            let phone = format!("+1-{:03}-{:03}-{:04}", rng.gen_range(200..999), rng.gen_range(100..999), rng.gen_range(1000..9999));
            output = output.replacen("{{phone}}", &phone, 1);
        }
        while output.contains("{{credit_card}}") {
            let cc = format!("4{:03}-{:04}-{:04}-{:04}", rng.gen_range(100..999), rng.gen_range(1000..9999), rng.gen_range(1000..9999), rng.gen_range(1000..9999));
            output = output.replacen("{{credit_card}}", &cc, 1);
        }

        // 7. Regex: {{random_int(min, max)}}
        output = self.re_random_int.replace_all(&output, |caps: &regex::Captures| {
            let min = caps.get(1).and_then(|m| m.as_str().parse::<i64>().ok()).unwrap_or(0);
            let max = caps.get(2).and_then(|m| m.as_str().parse::<i64>().ok()).unwrap_or(100);
            let (low, high) = if min <= max { (min, max) } else { (max, min) };
            let val = rand::thread_rng().gen_range(low..=high);
            val.to_string()
        }).to_string();

        // 8. Regex: {{random_float(min, max, precision)}}
        output = self.re_random_float.replace_all(&output, |caps: &regex::Captures| {
            let min = caps.get(1).and_then(|m| m.as_str().parse::<f64>().ok()).unwrap_or(0.0);
            let max = caps.get(2).and_then(|m| m.as_str().parse::<f64>().ok()).unwrap_or(100.0);
            let precision = caps.get(3).and_then(|m| m.as_str().parse::<usize>().ok()).unwrap_or(2);
            let (low, high) = if min <= max { (min, max) } else { (max, min) };
            let val = rand::thread_rng().gen_range(low..=high);
            format!("{:.prec$}", val, prec = precision)
        }).to_string();

        // 9. Regex: {{choose(['opt1', 'opt2', 'opt3'])}}
        output = self.re_choose.replace_all(&output, |caps: &regex::Captures| {
            let raw_list = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let items: Vec<String> = raw_list
                .split(',')
                .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if let Some(choice) = items.choose(&mut rand::thread_rng()) {
                choice.clone()
            } else {
                "".to_string()
            }
        }).to_string();

        output
    }
}
