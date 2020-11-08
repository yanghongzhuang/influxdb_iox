#![allow(dead_code)]
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use rand_distr::{Distribution, Normal};

use arrow_deps::parquet::data_type::ByteArray;
use packers::{sorter, Packer, Packers};

const ONE_MS: i64 = 1_000_000;
const ONE_HOUR: i64 = ONE_MS * 3_600_000;
const START_TIME: i64 = 1604188800000000000_i64;

// determines how many rows will be in a single segment, which is set to one
// hour.
const ROWS_PER_HOUR: usize = 1_000_000;

// minimum and maximum number of spans in a trace
const SPANS_MIN: usize = 2;
const SPANS_MAX: usize = 30;

const HOURS: usize = 1;

fn main() {
    let mut rng = rand::thread_rng();

    let mut segment = generate_segment(START_TIME, ROWS_PER_HOUR, &mut rng);

    // env, data_centre, cluster, node_id, pod_id, user_id, request_id, time
    //
    // No point ordering on trace_id or span_id
    sorter::sort(&mut segment, &[0, 1, 2, 6, 7, 3, 4, 5, 10]).unwrap();
    print_segment(&mut segment);
}

fn generate_segment(start_time: i64, rows_per_hour: usize, rng: &mut ThreadRng) -> Vec<Packers> {
    let mut segment = Vec::new();

    // 9 tag columns: env, data_centre, cluster, user_id, request_id, trace_id, node_id, pod_id, span_id
    for _ in 0..9 {
        segment.push(Packers::String(Packer::<ByteArray>::new()));
    }

    // A duration "field" column
    segment.push(Packers::Integer(Packer::<i64>::new()));

    // A timestamp column.
    segment.push(Packers::Integer(Packer::<i64>::new()));

    let traces_to_generate = rows_per_hour / ((SPANS_MIN + SPANS_MAX) / 2);
    let trace_start_time_spacing = (ONE_HOUR - (1000 * ONE_MS)) / traces_to_generate as i64; // back off a second to not overlap boundary

    // println!(
    //     "generating {:?} traces with spacing {:?} ns",
    //     traces_to_generate, trace_start_time_spacing
    // );
    for i in 0..traces_to_generate as i64 {
        // append a trace to the columns in a segment.
        //
        // For each trace take the hour "start time" and spread each trace
        // evenly through that hour.
        segment = generate_trace(
            start_time + (i * trace_start_time_spacing),
            rng.gen_range(SPANS_MIN, SPANS_MAX + 1),
            rng,
            segment,
        );
    }

    segment
}

fn generate_trace(
    timestamp: i64,
    spans: usize,
    rng: &mut ThreadRng,
    mut table: Vec<Packers>,
) -> Vec<Packers> {
    println!("START: {:?}", timestamp);
    let env_value = rng.gen_range(0_u8, 2);
    let env = format!("env-{:?}", env_value); // cardinality of 2.

    let data_centre_value = rng.gen_range(0_u8, 10);
    let data_centre = format!("data_centre-{:?}-{:?}", env_value, data_centre_value); // cardinality of 2 * 10  = 20

    let cluster_value = rng.gen_range(0_u8, 10);
    let cluster = format!(
        "cluster-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value // cardinality of 2 * 10 * 10 = 200
    );

    // user id is dependent on the cluster
    let user_id_value = rng.gen_range(0_u32, 1000);
    let user_id = format!(
        "uid-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value // cardinality of 2 * 10 * 10 * 1000 = 200,000
    );

    let request_id_value = rng.gen_range(0_u32, 10);
    let request_id = format!(
        "rid-{:?}-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value,
        request_id_value // cardinality of 2 * 10 * 10 * 1000 * 10 = 2,000,000
    );

    let trace_id = rng.sample_iter(&Alphanumeric).take(8).collect::<String>();

    let fixed_values = vec![
        ByteArray::from(env.as_str()),
        ByteArray::from(data_centre.as_str()),
        ByteArray::from(cluster.as_str()),
        ByteArray::from(user_id.as_str()),
        ByteArray::from(request_id.as_str()),
        ByteArray::from(trace_id.as_str()),
    ];
    let node_id_col = fixed_values.len();
    let pod_id_col = node_id_col + 1;
    let span_id_col = node_id_col + 2;
    let duration_field_col = node_id_col + 3;
    let timestamp_col = node_id_col + 4;

    // for the first 6 columns append the fixed generated values "spans many" times
    // so that there are "spans new rows" in each of the columns.
    let mut buffer = Vec::with_capacity(spans);
    for (i, value) in fixed_values.into_iter().enumerate() {
        buffer.clear();
        buffer.resize(spans, value);

        table[i].str_packer_mut().extend_from_slice(&buffer);
    }

    // the trace should move across hosts, which in this example would be
    // nodes and pods.
    buffer.clear(); // use this buffer for node_id column.
    let mut pod_buffer = Vec::with_capacity(spans);
    let node_id_prefix = format!("{}-{}-{}", env_value, data_centre_value, cluster_value,);
    for _ in 0..spans {
        // each node the trace hits is in the same cluster...
        let node_id = rng.gen_range(0, 10); // cardinality is 2 * 10 * 10 * 10 = 2,000
        buffer.push(ByteArray::from(
            format!("node_id-{}-{}", node_id_prefix, node_id).as_str(),
        ));

        pod_buffer.push(ByteArray::from(
            format!(
                "pod_id-{}-{}-{}",
                node_id_prefix,
                node_id,
                rng.gen_range(0, 10) // cardinality is 2 * 10 * 10 * 10 * 10 = 20,000
            )
            .as_str(),
        ));
    }
    table[node_id_col]
        .str_packer_mut()
        .extend_from_slice(&buffer);
    table[pod_id_col]
        .str_packer_mut()
        .extend_from_slice(&pod_buffer);

    // randomly generate span ids.
    buffer.clear();
    for _ in 0..spans {
        let id = rng.sample_iter(&Alphanumeric).take(8).collect::<String>();
        buffer.push(ByteArray::from(id.as_str()));
    }
    table[span_id_col]
        .str_packer_mut()
        .extend_from_slice(&buffer);

    // randomly generate some duration times in milliseconds.
    let normal = Normal::new(10.0, 5.0).unwrap();
    let durations = (0..spans)
        .map(|_| {
            (normal.sample(rng) * ONE_MS as f64)
                .max(ONE_MS as f64) // minimum duration is 1ms
                .round() as i64
        })
        .collect::<Vec<_>>();
    table[duration_field_col]
        .i64_packer_mut()
        .extend_from_slice(&durations);

    // write same timestamp.
    let mut times = std::iter::repeat(timestamp).take(spans).collect::<Vec<_>>();

    // adjust times by the durations.
    let mut total_duration = 0;
    for i in 0..times.len() {
        times[i] += total_duration;
        total_duration += durations[i];
    }
    table[timestamp_col]
        .i64_packer_mut()
        .extend_from_slice(&times);

    // Return the updated table back
    table
}

fn print_segment(segment: &mut Vec<Packers>) {
    let total_rows = segment[0].num_rows();
    let mut rows = 0;

    let mut col_itrs = segment.iter_mut().map(|p| p.iter()).collect::<Vec<_>>();

    while rows < total_rows {
        if rows > 0 {
            println!();
        }

        for itr in col_itrs.iter_mut() {
            match itr {
                packers::packers::PackersIterator::Float(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Integer(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::String(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v.as_utf8().unwrap()),
                            None => print!("NULL"),
                        }
                    }
                }
                packers::packers::PackersIterator::Boolean(itr) => {
                    if let Some(v) = itr.next() {
                        match v {
                            Some(v) => print!("{},", v),
                            None => print!("NULL"),
                        }
                    }
                }
            }
        }
        rows += 1;
    }
}
