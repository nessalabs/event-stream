mod ui;
use event_stream_verification::{
    catalog,
    runner::{self},
};
use gpui_kit::component::{Root, Theme, ThemeMode};
use gpui_kit::*;
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !args.is_empty() {
        std::process::exit(headless(&args));
    }
    gpui_kit::application()
        .with_assets(gpui_kit::assets::Assets)
        .run(|cx| {
            gpui_kit::init(cx);
            Theme::change(ThemeMode::Dark, None, cx);
            cx.activate(true);
            let options = WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                    None,
                    size(px(1280.), px(850.)),
                    cx,
                ))),
                titlebar: Some(TitlebarOptions {
                    title: Some("Event stream · Verification".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            cx.spawn(async move |cx| {
                cx.open_window(options, |window, cx| {
                    let root = runner::workspace();
                    let executor = Arc::new(runner::RegisteredExecutor {
                        workspace: root.clone(),
                    });
                    let evidence = Arc::new(runner::FileEvidenceStore {
                        workspace: root.clone(),
                    });
                    let view = cx.new(|_| ui::VerificationApp::new(root, executor, evidence));
                    cx.new(|cx| Root::new(view, window, cx))
                })
                .expect("failed to open verification window");
            })
            .detach();
        });
}

fn headless(args: &[String]) -> i32 {
    if args == ["--list"] {
        for stream in catalog::workstreams() {
            println!("{:04} {}", stream.number, stream.title);
            for s in stream.scenarios {
                println!(
                    "  {:24} {}  {}",
                    s.id,
                    if s.run.is_some() {
                        "ready  "
                    } else {
                        "blocked"
                    },
                    s.target
                );
            }
        }
        return 0;
    }
    if args.len() != 2 || args[0] != "--scenario" {
        eprintln!("Usage: event-stream-verification [--list | --scenario <id>]");
        return 2;
    }
    let root = runner::workspace();
    let executor = runner::RegisteredExecutor {
        workspace: root.clone(),
    };
    let evidence = runner::FileEvidenceStore { workspace: root };
    match runner::run_and_save(&executor, &evidence, &args[1]) {
        Ok(completion) => {
            let report = completion.report;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if report.status == runner::Status::Blocked {
                return 2;
            }
            if let Err(error) = completion.saved {
                eprintln!("Evidence save failed: {error}");
                return 1;
            }
            if report.status == runner::Status::Passed {
                0
            } else {
                1
            }
        }
        Err(error) => {
            eprintln!("{error}");
            2
        }
    }
}
