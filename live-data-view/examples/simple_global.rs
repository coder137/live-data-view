use std::time::Duration;

use live_data_view::DataViewSystem;

fn main() {
    DataViewSystem::init_global().unwrap();
    let dataview_system = DataViewSystem::get_global().unwrap();

    let mut event = dataview_system.get_event_channel();
    std::thread::spawn(move || {
        loop {
            let data = event.blocking_recv();
            println!("Data: {data:?}");
            if data.is_err() {
                break;
            }
        }
    });

    let data = 0u32;
    let mut data = dataview_system.register_read(data).unwrap();

    for i in 0..10 {
        *data = i * 3;
        data.notify();
        std::thread::sleep(Duration::from_millis(500));
    }
    println!("DONE");
}
