use event_stream_verification::{
    catalog::{self, Workstream},
    performance::{ChartPoint, ExperimentRow, ExperimentStatus, HomeViewModel, RoundStatus},
    runner::{run_and_save, EvidenceStore, RunReport, ScenarioExecutor, Status},
};
use gpui_kit::component::{button::*, scroll::ScrollableElement, *};
use gpui_kit::prelude::FluentBuilder;
use gpui_kit::*;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Evidence,
    Contract,
}

#[derive(Clone, Copy, PartialEq)]
enum Screen {
    Home,
    Scenario,
}

pub struct VerificationApp {
    root: PathBuf,
    streams: Vec<Workstream>,
    screen: Screen,
    home: Result<HomeViewModel, String>,
    home_selected: usize,
    home_page: usize,
    selected: (usize, usize),
    expanded: Vec<bool>,
    sidebar_open: bool,
    tab: Tab,
    reports: HashMap<String, RunReport>,
    notices: HashMap<String, String>,
    running: Option<String>,
    executor: Arc<dyn ScenarioExecutor>,
    evidence: Arc<dyn EvidenceStore>,
}

impl VerificationApp {
    pub fn new(
        root: PathBuf,
        executor: Arc<dyn ScenarioExecutor>,
        evidence: Arc<dyn EvidenceStore>,
    ) -> Self {
        let streams = catalog::workstreams();
        let mut reports = HashMap::new();
        let mut notices = HashMap::new();
        for scenario in streams.iter().flat_map(|s| &s.scenarios) {
            match evidence.load(scenario) {
                Ok(Some(report)) => {
                    reports.insert(scenario.id.into(), report);
                }
                Err(error) => {
                    notices.insert(scenario.id.into(), error);
                }
                _ => {}
            }
        }
        let home = HomeViewModel::load(&root);
        let home_selected = home
            .as_ref()
            .map(HomeViewModel::default_row_index)
            .unwrap_or(0);
        Self {
            expanded: vec![true; streams.len()],
            streams,
            root,
            screen: Screen::Home,
            home,
            home_selected,
            home_page: home_selected / 50,
            selected: (0, 0),
            sidebar_open: false,
            tab: Tab::Evidence,
            reports,
            notices,
            running: None,
            executor,
            evidence,
        }
    }

    fn run_selected(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let scenario = &self.streams[self.selected.0].scenarios[self.selected.1];
        if self.running.is_some() || scenario.run.is_none() {
            return;
        }
        let id = scenario.id.to_string();
        self.running = Some(id.clone());
        self.notices.remove(&id);
        let executor = self.executor.clone();
        let evidence = self.evidence.clone();
        let task = cx.background_executor().spawn(async move {
            let result = run_and_save(executor.as_ref(), evidence.as_ref(), &id);
            (id, result)
        });
        cx.spawn(async move |this, cx| {
            let (id, result) = task.await;
            let _ = this.update(cx, |app, cx| {
                app.running = None;
                match result {
                    Ok(completion) => {
                        app.reports.insert(id.clone(), completion.report);
                        if let Err(error) = completion.saved {
                            app.notices.insert(
                                id,
                                format!("Run completed, but evidence was not saved: {error}"),
                            );
                        }
                    }
                    Err(error) => {
                        app.notices.insert(id, error);
                    }
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn sidebar(&self, cx: &mut Context<Self>) -> Div {
        let mut folders = div().v_flex().gap_3();
        for (wi, stream) in self.streams.iter().enumerate() {
            let is_open = self.expanded[wi];
            let selected = self.screen == Screen::Scenario && self.selected.0 == wi;
            let mut folder = div().v_flex().gap_1().child(
                div()
                    .id(format!("folder-{wi}"))
                    .h_flex()
                    .items_center()
                    .gap_2()
                    .px_2()
                    .py_2()
                    .rounded_md()
                    .cursor_pointer()
                    .text_sm()
                    .text_color(if selected {
                        cx.theme().foreground
                    } else {
                        cx.theme().muted_foreground
                    })
                    .hover(|s| s.bg(cx.theme().accent))
                    .child(if is_open { "⌄" } else { "›" })
                    .child(div().text_xs().child(format!("{:02}", stream.number)))
                    .child(stream.title)
                    .on_click(cx.listener(move |app, _, _, cx| {
                        app.expanded[wi] = !app.expanded[wi];
                        cx.notify();
                    })),
            );
            if is_open {
                for (si, scenario) in stream.scenarios.iter().enumerate() {
                    let active = self.screen == Screen::Scenario && self.selected == (wi, si);
                    let (label, color) = if self.running.as_deref() == Some(scenario.id) {
                        ("◌", color(0xdab974))
                    } else if let Some(report) = self.reports.get(scenario.id) {
                        match report.status {
                            Status::Passed => ("●", color(0x70c5a0)),
                            Status::Failed => ("●", color(0xe58b8b)),
                            Status::Blocked => ("○", cx.theme().muted_foreground),
                        }
                    } else if scenario.run.is_some() {
                        ("○", color(0x89acd8))
                    } else {
                        ("·", cx.theme().muted_foreground)
                    };
                    folder = folder.child(
                        div()
                            .id(format!("scenario-{}", scenario.id))
                            .h_flex()
                            .items_center()
                            .gap_2()
                            .ml_5()
                            .px_3()
                            .py_2()
                            .rounded_md()
                            .cursor_pointer()
                            .text_sm()
                            .when(active, |s| s.bg(cx.theme().accent))
                            .hover(|s| s.bg(cx.theme().accent))
                            .child(div().text_color(color).child(label))
                            .child(div().flex_1().child(scenario.title))
                            .on_click(cx.listener(move |app, _, _, cx| {
                                app.selected = (wi, si);
                                app.screen = Screen::Scenario;
                                app.tab = Tab::Evidence;
                                app.sidebar_open = false;
                                cx.notify();
                            })),
                    );
                }
            }
            folders = folders.child(folder);
        }
        div()
            .v_flex()
            .w(px(295.))
            .h_full()
            .flex_none()
            .bg(cx.theme().sidebar)
            .border_r_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .id("home")
                    .h_flex()
                    .items_center()
                    .gap_2()
                    .mx_2()
                    .mb_2()
                    .px_3()
                    .py_2()
                    .rounded_md()
                    .cursor_pointer()
                    .text_sm()
                    .when(self.screen == Screen::Home, |s| s.bg(cx.theme().accent))
                    .hover(|s| s.bg(cx.theme().accent))
                    .child("⌂")
                    .child("Home · experiment rounds")
                    .on_click(cx.listener(|app, _, _, cx| {
                        app.screen = Screen::Home;
                        app.sidebar_open = false;
                        cx.notify();
                    })),
            )
            .child(
                div()
                    .v_flex()
                    .gap_2()
                    .px_5()
                    .pt_6()
                    .pb_4()
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Event stream"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child("Verification workspace"),
                    ),
            )
            .child(
                div()
                    .h_flex()
                    .justify_between()
                    .px_5()
                    .py_2()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child("WORKSTREAMS")
                    .child("10"),
            )
            .child(
                div()
                    .id("workstreams-scroll")
                    .v_flex()
                    .flex_1()
                    .min_h_0()
                    .px_2()
                    .pb_4()
                    .overflow_y_scrollbar()
                    .child(folders),
            )
            .child(
                div()
                    .v_flex()
                    .gap_2()
                    .p_4()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("● Passed    ○ Ready    · Planned"),
                    )
                    .child(
                        Button::new("roadmap")
                            .ghost()
                            .small()
                            .label("Open ADR roadmap")
                            .on_click(cx.listener(|app, _, _, cx| {
                                cx.reveal_path(&app.root.join("docs/adr/README.md"))
                            })),
                    ),
            )
    }

    fn content(&self, compact: bool, cx: &mut Context<Self>) -> Div {
        let stream = &self.streams[self.selected.0];
        let scenario = &stream.scenarios[self.selected.1];
        let report = self.reports.get(scenario.id);
        let running = self.running.as_deref() == Some(scenario.id);
        let status = if running {
            "Running"
        } else if scenario.run.is_none() {
            "Not implemented"
        } else {
            report.map(|r| r.status.label()).unwrap_or("Ready to run")
        };
        let tone = if running {
            color(0xdab974)
        } else {
            report
                .map(|r| {
                    if r.status == Status::Passed {
                        color(0x70c5a0)
                    } else {
                        color(0xe58b8b)
                    }
                })
                .unwrap_or(cx.theme().muted_foreground)
        };
        let mut body = div()
            .v_flex()
            .gap_5()
            .p_6()
            .child(
                div()
                    .h_flex()
                    .items_center()
                    .gap_3()
                    .child(badge(status, tone))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(if scenario.run.is_some() {
                                "REGISTERED CHECK · REAL CALLBACK"
                            } else {
                                "PRODUCTION CHECK · PLANNED"
                            }),
                    ),
            )
            .child(
                div()
                    .v_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_2xl()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(scenario.title),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(scenario.purpose),
                    ),
            )
            .child(
                div()
                    .h_flex()
                    .gap_2()
                    .child(
                        Button::new("evidence-tab")
                            .ghost()
                            .small()
                            .label("Evidence")
                            .toggled(self.tab == Tab::Evidence)
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.tab = Tab::Evidence;
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("contract-tab")
                            .ghost()
                            .small()
                            .label("Input & contract")
                            .toggled(self.tab == Tab::Contract)
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.tab = Tab::Contract;
                                cx.notify();
                            })),
                    ),
            );
        if let Some(notice) = self.notices.get(scenario.id) {
            body = body.child(card(
                "EVIDENCE NOTICE",
                div().text_sm().child(notice.clone()),
                cx,
            ));
        }
        if self.tab == Tab::Contract {
            body = body
                .child(card(
                    "INPUT FIXTURE",
                    code(&serde_json::to_string_pretty(&scenario.input).unwrap(), cx),
                    cx,
                ))
                .child(card(
                    "EXPECTED BEHAVIOR",
                    div().text_sm().child(scenario.expected),
                    cx,
                ))
                .child(card(
                    "IMPLEMENTATION TARGET",
                    div()
                        .v_flex()
                        .gap_3()
                        .child(code(scenario.target, cx))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(if scenario.run.is_some() {
                                    scenario.source
                                } else {
                                    "Planned target. No production source file is registered yet."
                                }),
                        ),
                    cx,
                ));
        } else {
            let elapsed = report
                .map(|r| format!("{:.2} ms", r.elapsed_us as f64 / 1000.))
                .unwrap_or_else(|| "—".into());
            let checks = report
                .map(|r| {
                    format!(
                        "{} / {}",
                        r.result.observations.iter().filter(|o| o.passed).count(),
                        r.result.observations.len()
                    )
                })
                .unwrap_or_else(|| "—".into());
            body = body.child(
                div()
                    .h_flex()
                    .gap_3()
                    .child(metric("CHECKS PASSED", &checks, "Observed assertions", cx))
                    .child(metric(
                        "EXECUTION TIME",
                        &elapsed,
                        "Wall time · excludes save / UI",
                        cx,
                    ))
                    .child(metric(
                        "CPU / MEMORY",
                        "Not sampled",
                        "No inferred measurements",
                        cx,
                    )),
            );
            if scenario.run.is_none() {
                body=body.child(card("IMPLEMENTATION NEEDED",div().v_flex().gap_3()
                    .child(div().text_sm().child(scenario.blocked_reason))
                    .child(code(scenario.target,cx))
                    .child(div().text_sm().text_color(cx.theme().muted_foreground).child("The input and expected outcome are ready to review in Input & contract. Run becomes available when a callable scenario is registered.")),cx));
            } else if let Some(report) = report {
                body = body.child(card(
                    "LATEST OBSERVATION",
                    div()
                        .v_flex()
                        .gap_2()
                        .child(div().text_sm().child(report.result.summary.clone()))
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(report.evidence_scope.clone()),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(format!(
                                    "Run · {} · Unix time {} ms",
                                    report.run_id, report.started_unix_ms
                                )),
                        ),
                    cx,
                ));
                let mut rows = div().v_flex().gap_2();
                for (index, observation) in report.result.observations.iter().enumerate() {
                    rows = rows.child(
                        div()
                            .v_flex()
                            .gap_1()
                            .p_3()
                            .rounded_md()
                            .bg(cx.theme().secondary)
                            .child(
                                div()
                                    .h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        div()
                                            .text_color(if observation.passed {
                                                color(0x70c5a0)
                                            } else {
                                                color(0xe58b8b)
                                            })
                                            .child(if observation.passed { "✓" } else { "×" }),
                                    )
                                    .child(
                                        div().text_sm().font_weight(FontWeight::MEDIUM).child(
                                            format!("{:02}  {}", index + 1, observation.label),
                                        ),
                                    ),
                            )
                            .child(div().text_sm().child(observation.actual.clone()))
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(format!("Expected: {}", observation.expected)),
                            ),
                    );
                }
                body = body.child(card("OBSERVED CHECKS", rows, cx));
                let output = serde_json::to_string_pretty(&report.result.output).unwrap();
                body = body.child(card(
                    "ACTUAL OUTPUT",
                    div().child(
                        div()
                            .id(format!("actual-output-{}", scenario.id))
                            .max_h(px(280.))
                            .overflow_y_scrollbar()
                            .child(code(&output, cx)),
                    ),
                    cx,
                ));
            } else {
                body=body.child(card("READY FOR FIRST RUN",div().v_flex().gap_3()
                    .child(div().text_sm().child("Run calls the registered Rust function on a background worker. Review each assertion and the returned data here."))
                    .child(code(scenario.target,cx))
                    .child(div().text_sm().text_color(cx.theme().muted_foreground).child(scenario.evidence_scope)),cx));
            }
            let columns = if compact {
                div().v_flex()
            } else {
                div().h_flex()
            };
            body = body.child(
                columns
                    .gap_3()
                    .child(card(
                        "INPUT",
                        code(&serde_json::to_string_pretty(&scenario.input).unwrap(), cx),
                        cx,
                    ))
                    .child(card(
                        "EXPECTED",
                        div().text_sm().child(scenario.expected),
                        cx,
                    )),
            );
        }
        body.child(
            div()
                .h_flex()
                .gap_2()
                .child(
                    Button::new("open-contract")
                        .ghost()
                        .small()
                        .label("Reveal ADR")
                        .on_click(cx.listener(|app, _, _, cx| {
                            cx.reveal_path(&app.root.join(app.streams[app.selected.0].document()));
                        })),
                )
                .child(
                    Button::new("source")
                        .ghost()
                        .small()
                        .label("Reveal implementation")
                        .disabled(scenario.source.is_empty())
                        .on_click(cx.listener(|app, _, _, cx| {
                            let s = &app.streams[app.selected.0].scenarios[app.selected.1];
                            if !s.source.is_empty() {
                                cx.reveal_path(&app.root.join(s.source));
                            }
                        })),
                )
                .child(
                    Button::new("copy-evidence")
                        .ghost()
                        .small()
                        .label("Copy evidence JSON")
                        .disabled(report.is_none())
                        .on_click(cx.listener(|app, _, _, cx| {
                            let s = &app.streams[app.selected.0].scenarios[app.selected.1];
                            if let Some(r) = app.reports.get(s.id) {
                                cx.write_to_clipboard(ClipboardItem::new_string(
                                    serde_json::to_string_pretty(r).unwrap(),
                                ));
                            }
                        })),
                ),
        )
    }

    fn refresh_home(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let prior = self.home.as_ref().ok().and_then(|home| {
            home.rows
                .get(self.home_selected)
                .map(|row| (row.round_number, row.experiment.id.clone()))
        });
        self.home = HomeViewModel::load(&self.root);
        self.home_selected = self.home.as_ref().map_or(0, |home| {
            prior
                .as_ref()
                .and_then(|(round, id)| {
                    home.rows
                        .iter()
                        .position(|row| row.round_number == *round && row.experiment.id == *id)
                })
                .unwrap_or_else(|| home.default_row_index())
        });
        self.home_page = self.home_selected / 50;
        cx.notify();
    }

    fn home_content(&self, compact: bool, cx: &mut Context<Self>) -> Div {
        let mut body = div().v_flex().gap_5().p_6().child(
            div()
                .v_flex()
                .gap_2()
                .child(
                    div()
                        .text_2xl()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child("Measured experiment rounds"),
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child("Local build iterations backed by saved evidence. These numbers are not conversation turn IDs."),
                ),
        );
        let home = match &self.home {
            Ok(home) => home,
            Err(error) => {
                return body.child(card(
                    "HOME EVIDENCE UNAVAILABLE",
                    div().v_flex().gap_2().child(error.clone()).child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child("Refresh only reads verification/evidence/home.json. It never starts a benchmark."),
                    ),
                    cx,
                ));
            }
        };
        body = body.child(
            div()
                .h_flex()
                .flex_wrap()
                .gap_4()
                .px_3()
                .py_2()
                .rounded_md()
                .bg(cx.theme().secondary)
                .text_xs()
                .child(format!("{} rounds", round_count(home)))
                .child(format!("{} experiments", home.rows.len()))
                .child(format!(
                    "Catalog updated {}",
                    short_utc(&home.generated_at_utc)
                )),
        );
        if let Some(latest) = home.rounds.last() {
            body = body.child(card(
                "LATEST WORK ROUND",
                div()
                    .h_flex()
                    .items_start()
                    .gap_3()
                    .child(badge(
                        &format!(
                            "Round {:02} · {}",
                            latest.number,
                            round_status(latest.status)
                        ),
                        cx.theme().muted_foreground,
                    ))
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .text_sm()
                            .child(latest.summary.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("{} rows", latest.experiment_count)),
                    ),
                cx,
            ));
        }
        for issue in &home.issues {
            body = body.child(card(
                "EVIDENCE NOTICE",
                div().text_sm().child(issue.clone()),
                cx,
            ));
        }

        if let Some(selected) = home.rows.get(self.home_selected) {
            let runtime = home.runtime_series(self.home_selected);
            let memory = home.memory_series(self.home_selected);
            let key = &selected.experiment.comparison_key;
            let phase = key.measurement_phase.replace('_', " ");
            let runtime_title = format!("RUNTIME · {phase}");
            let memory_title = format!("RSS · {phase}");
            let charts = if compact {
                div().v_flex()
            } else {
                div().h_flex()
            };
            body = body.child(
                charts
                    .gap_3()
                    .child(chart(
                        &runtime_title,
                        &runtime,
                        ChartUnit::Duration,
                        "wall-clock median",
                        cx,
                    ))
                    .child(chart(
                        &memory_title,
                        &memory,
                        ChartUnit::Bytes,
                        &key.memory_scope.replace('_', " "),
                        cx,
                    )),
            );
            body = body.child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(format!(
                        "Selected {:02}.{:02} · {}",
                        selected.round_number,
                        selected.experiment_number,
                        selected.experiment.label
                    )),
            );
        }

        const PAGE_SIZE: usize = 50;
        let page_count = home.rows.len().div_ceil(PAGE_SIZE).max(1);
        let page = self.home_page.min(page_count - 1);
        let start = page * PAGE_SIZE;
        let end = (start + PAGE_SIZE).min(home.rows.len());
        let mut table = div().v_flex().gap_1();
        if !compact {
            table = table.child(home_table_header(cx));
        }
        for index in start..end {
            table = table.child(self.home_table_row(index, compact, cx));
        }
        if home.rows.is_empty() {
            table = table.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("No experiment rows are recorded yet."),
            );
        }
        body = body.child(card(
            "EXPERIMENT HISTORY",
            div().v_flex().gap_3().child(table).child(
                div()
                    .h_flex()
                    .items_center()
                    .justify_between()
                    .child(
                        Button::new("home-previous")
                            .ghost()
                            .small()
                            .label("Previous")
                            .disabled(page == 0)
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.home_page = app.home_page.saturating_sub(1);
                                cx.notify();
                            })),
                    )
                    .child(format!(
                        "Page {} of {} · {} rows",
                        page + 1,
                        page_count,
                        home.rows.len()
                    ))
                    .child(
                        Button::new("home-next")
                            .ghost()
                            .small()
                            .label("Next")
                            .disabled(page + 1 >= page_count)
                            .on_click(cx.listener(move |app, _, _, cx| {
                                app.home_page = (app.home_page + 1).min(page_count - 1);
                                cx.notify();
                            })),
                    ),
            ),
            cx,
        ));

        if let Some(selected) = home.rows.get(self.home_selected) {
            body = body.child(self.home_detail(selected, cx));
        }
        body
    }

    fn home_table_row(&self, index: usize, compact: bool, cx: &mut Context<Self>) -> AnyElement {
        let home = self
            .home
            .as_ref()
            .expect("Home rows require loaded evidence");
        let row = &home.rows[index];
        let status = experiment_status(row);
        let runtime = format_optional_duration(row.experiment.metrics.runtime_ns_median);
        let cpu = format_optional_cpu(row.experiment.metrics.cpu_us_median);
        let rss = format_optional_bytes(row.experiment.metrics.memory_bytes_median);
        let agents = format_optional_count(row.experiment.metrics.agents);
        let outcomes = format_outcomes(row);
        let selected = index == self.home_selected;
        let base = div()
            .id(format!("experiment-row-{index}"))
            .rounded_md()
            .cursor_pointer()
            .when(selected, |s| s.bg(cx.theme().accent))
            .hover(|s| s.bg(cx.theme().accent))
            .on_click(cx.listener(move |app, _, _, cx| {
                app.home_selected = index;
                cx.notify();
            }));
        if compact {
            base.v_flex()
                .gap_1()
                .p_3()
                .child(format!("{:02}.{:02} · {}", row.round_number, row.experiment_number, row.experiment.label))
                .child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().muted_foreground)
                        .child(format!("{status} · runtime {runtime} · CPU {cpu} · RSS {rss} · agents {agents} · {outcomes}")),
                )
                .into_any_element()
        } else {
            base.h_flex()
                .items_center()
                .gap_2()
                .px_2()
                .py_2()
                .text_xs()
                .child(div().w(px(74.)).child(format!(
                    "{:02}.{:02}",
                    row.round_number, row.experiment_number
                )))
                .child(
                    div()
                        .w(px(185.))
                        .truncate()
                        .child(row.experiment.label.clone()),
                )
                .child(div().w(px(82.)).child(status))
                .child(div().w(px(92.)).child(runtime))
                .child(div().w(px(82.)).child(cpu))
                .child(div().w(px(92.)).child(rss))
                .child(div().w(px(65.)).child(agents))
                .child(div().flex_1().min_w_0().truncate().child(outcomes))
                .into_any_element()
        }
    }

    fn home_detail(&self, row: &ExperimentRow, cx: &mut Context<Self>) -> Div {
        let raw = serde_json::to_string_pretty(&row.experiment)
            .unwrap_or_else(|error| format!("Could not render experiment: {error}"));
        let source = row
            .experiment
            .provenance
            .source_path
            .as_deref()
            .unwrap_or("No source artifact recorded");
        div()
            .v_flex()
            .gap_3()
            .child(card(
                "SELECTED EXPERIMENT",
                div()
                    .v_flex()
                    .gap_2()
                    .child(format!(
                        "Experiment round {:02} · {}",
                        row.round_number, row.experiment.label
                    ))
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!(
                                "{} · {}",
                                row.round_summary,
                                experiment_status(row)
                            )),
                    )
                    .child(div().text_xs().child(format!(
                        "Build: {}",
                        row.build_id.as_deref().unwrap_or("not recorded")
                    )))
                    .child(
                        div()
                            .text_xs()
                            .child(format!("Recorded source path: {source}")),
                    )
                    .child(div().text_xs().child(format!(
                        "Recorded source SHA: {}",
                        row.experiment
                            .provenance
                            .source_sha256
                            .as_deref()
                            .unwrap_or("not recorded")
                    ))),
                cx,
            ))
            .child(card(
                "WORKLOAD, BUILD, SCOPE, AND RAW CATALOG ENTRY",
                div().child(
                    div()
                        .id("selected-experiment-raw")
                        .max_h(px(360.))
                        .overflow_y_scrollbar()
                        .child(code(&raw, cx)),
                ),
                cx,
            ))
    }
}

impl Render for VerificationApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let narrow = window.viewport_size().width < px(980.);
        let show_sidebar = !narrow || self.sidebar_open;
        let (location, running, runnable, has_report) = if self.screen == Screen::Home {
            ("HOME  /  EXPERIMENT ROUNDS".to_string(), false, true, false)
        } else {
            let stream = &self.streams[self.selected.0];
            let scenario = &stream.scenarios[self.selected.1];
            (
                format!("{:04}  /  {}", stream.number, stream.title),
                self.running.as_deref() == Some(scenario.id),
                scenario.run.is_some(),
                self.reports.contains_key(scenario.id),
            )
        };
        let header = div()
            .h_flex()
            .items_center()
            .justify_between()
            .gap_3()
            .px_6()
            .py_4()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .h_flex()
                    .gap_3()
                    .items_center()
                    .when(narrow, |el| {
                        el.child(
                            Button::new("toggle-sidebar")
                                .ghost()
                                .small()
                                .label("Workstreams")
                                .on_click(cx.listener(|app, _, _, cx| {
                                    app.sidebar_open = !app.sidebar_open;
                                    cx.notify();
                                })),
                        )
                    })
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(location),
                    ),
            )
            .child(
                div()
                    .h_flex()
                    .gap_2()
                    .child(
                        Button::new("theme")
                            .ghost()
                            .small()
                            .label(if cx.theme().is_dark() {
                                "Light"
                            } else {
                                "Dark"
                            })
                            .on_click(cx.listener(|_, _, window, cx| {
                                let mode = if cx.theme().is_dark() {
                                    ThemeMode::Light
                                } else {
                                    ThemeMode::Dark
                                };
                                Theme::change(mode, Some(window), cx);
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("run")
                            .primary()
                            .small()
                            .label(if self.screen == Screen::Home {
                                "Refresh evidence"
                            } else if running {
                                "Running…"
                            } else if !runnable {
                                "Not implemented"
                            } else if has_report {
                                "Run again"
                            } else {
                                "Run verification"
                            })
                            .disabled(
                                self.screen == Screen::Scenario
                                    && (self.running.is_some() || !runnable),
                            )
                            .on_click(cx.listener(|app, event, window, cx| {
                                if app.screen == Screen::Home {
                                    app.refresh_home(event, window, cx);
                                } else {
                                    app.run_selected(event, window, cx);
                                }
                            })),
                    ),
            );
        div().h_flex().size_full().overflow_hidden().bg(cx.theme().background).text_color(cx.theme().foreground)
            .when(show_sidebar,|el|el.child(self.sidebar(cx)))
            .child(div().v_flex().flex_1().min_w_0().h_full().overflow_hidden()
                .child(header)
                .child(div().id("main-scroll").v_flex().flex_1().min_h_0().overflow_y_scrollbar().child(if self.screen == Screen::Home { self.home_content(narrow, cx) } else { self.content(narrow,cx) }))
                .child(div().h_flex().justify_between().px_6().py_2().border_t_1().border_color(cx.theme().border).text_xs().text_color(cx.theme().muted_foreground)
                    .child(if self.screen == Screen::Home { "Saved evidence only · Refresh does not run benchmarks".into() } else { self.running.as_ref().map(|id|format!("Running {id} · background worker")).unwrap_or_else(||"One scenario at a time · latest evidence retained per verification".into()) })
                    .child("Local workspace")))
    }
}

fn badge(label: &str, color: Hsla) -> Div {
    div()
        .px_2()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(color)
        .text_xs()
        .text_color(color)
        .child(label.to_string())
}
fn card(title: &str, content: Div, cx: &App) -> Div {
    div()
        .v_flex()
        .flex_1()
        .min_w_0()
        .gap_3()
        .p_4()
        .rounded_lg()
        .border_1()
        .border_color(cx.theme().border)
        .child(
            div()
                .text_xs()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(cx.theme().muted_foreground)
                .child(title.to_string()),
        )
        .child(content)
}
fn metric(label: &str, value: &str, detail: &str, cx: &App) -> Div {
    card(
        label,
        div()
            .v_flex()
            .gap_2()
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::MEDIUM)
                    .child(value.to_string()),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(detail.to_string()),
            ),
        cx,
    )
}

fn round_count(home: &HomeViewModel) -> usize {
    home.rounds.len()
}

fn short_utc(value: &str) -> String {
    value
        .get(..16)
        .unwrap_or(value)
        .replace('T', " ")
        .to_string()
}

fn round_status(status: RoundStatus) -> &'static str {
    match status {
        RoundStatus::Measured => "Measured",
        RoundStatus::Partial => "Partial",
        RoundStatus::NotMeasured => "Not measured",
        RoundStatus::PriorSource => "Prior source",
    }
}

fn experiment_status(row: &ExperimentRow) -> &'static str {
    match row.experiment.status {
        ExperimentStatus::Partial => "Partial",
        ExperimentStatus::NotMeasured => "Not measured",
        ExperimentStatus::Measured if row.round_status == RoundStatus::PriorSource => {
            "Prior source"
        }
        ExperimentStatus::Measured => "Measured",
    }
}

fn home_table_header(cx: &App) -> Div {
    div()
        .h_flex()
        .items_center()
        .gap_2()
        .px_2()
        .pb_2()
        .text_xs()
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(cx.theme().muted_foreground)
        .child(div().w(px(74.)).flex_none().child("EXPT"))
        .child(div().w(px(185.)).child("EXPERIMENT"))
        .child(div().w(px(82.)).child("STATUS"))
        .child(div().w(px(92.)).child("RUNTIME"))
        .child(div().w(px(82.)).child("CPU"))
        .child(div().w(px(92.)).child("RSS"))
        .child(div().w(px(65.)).child("AGENTS"))
        .child(div().flex_1().child("OUTCOME A/R/F/D"))
}

#[derive(Clone, Copy)]
enum ChartUnit {
    Duration,
    Bytes,
}

fn chart(title: &str, points: &[ChartPoint], unit: ChartUnit, scope: &str, cx: &App) -> Div {
    let max = points.iter().map(|point| point.value).max().unwrap_or(1);
    let mut rows = div().v_flex().gap_2().child(
        div()
            .text_xs()
            .text_color(cx.theme().muted_foreground)
            .child(format!("Scope: {scope}")),
    );
    for point in points {
        let width = (point.value as f64 / max as f64 * 190.0).max(4.0) as f32;
        let delta = match point.baseline_delta_percent {
            None => "change unavailable · zero baseline".to_string(),
            Some(0.0) => "baseline".to_string(),
            Some(value) if value < 0.0 => format!("{value:.1}%"),
            Some(value) => format!("+{value:.1}% regression"),
        };
        let tone = if point
            .baseline_delta_percent
            .is_some_and(|value| value > 0.0)
        {
            color(0xe58b8b)
        } else {
            color(0x70c5a0)
        };
        rows = rows.child(
            div()
                .v_flex()
                .gap_1()
                .child(
                    div()
                        .h_flex()
                        .justify_between()
                        .text_xs()
                        .child(format!(
                            "Round {:02} · {}",
                            point.round_number,
                            match unit {
                                ChartUnit::Duration => format_optional_duration(Some(point.value)),
                                ChartUnit::Bytes => format_optional_bytes(Some(point.value)),
                            }
                        ))
                        .child(div().text_color(tone).child(delta)),
                )
                .child(div().h(px(7.)).w(px(width)).rounded_md().bg(tone)),
        );
    }
    if points.is_empty() {
        rows = rows.child(
            div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child("No measured value for this workload."),
        );
    } else if points.len() == 1 {
        rows = rows.child(
            div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child("No comparable earlier round"),
        );
    }
    card(title, rows, cx)
}

fn format_optional_duration(value: Option<u64>) -> String {
    value
        .map(|nanoseconds| {
            if nanoseconds >= 1_000_000_000 {
                format!("{:.2}s", nanoseconds as f64 / 1_000_000_000.0)
            } else if nanoseconds >= 1_000_000 {
                format!("{:.2}ms", nanoseconds as f64 / 1_000_000.0)
            } else {
                format!("{nanoseconds}ns")
            }
        })
        .unwrap_or_else(|| "Not measured".into())
}

fn format_optional_cpu(value: Option<u64>) -> String {
    value
        .map(|microseconds| {
            if microseconds >= 1_000_000 {
                format!("{:.2}s", microseconds as f64 / 1_000_000.0)
            } else if microseconds >= 1_000 {
                format!("{:.2}ms", microseconds as f64 / 1_000.0)
            } else {
                format!("{microseconds}µs")
            }
        })
        .unwrap_or_else(|| "Not measured".into())
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value
        .map(|bytes| {
            if bytes >= 1024 * 1024 {
                format!("{:.1}MiB", bytes as f64 / (1024.0 * 1024.0))
            } else if bytes >= 1024 {
                format!("{:.1}KiB", bytes as f64 / 1024.0)
            } else {
                format!("{bytes}B")
            }
        })
        .unwrap_or_else(|| "Not measured".into())
}

fn format_optional_count(value: Option<u64>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "—".into())
}

fn format_outcomes(row: &ExperimentRow) -> String {
    let metrics = &row.experiment.metrics;
    if [
        metrics.accepted,
        metrics.rejected,
        metrics.failed,
        metrics.delivered,
    ]
    .iter()
    .all(Option::is_none)
    {
        return "Not measured".into();
    }
    format!(
        "{}/{}/{}/{}",
        format_optional_count(metrics.accepted),
        format_optional_count(metrics.rejected),
        format_optional_count(metrics.failed),
        format_optional_count(metrics.delivered)
    )
}

fn code(text: &str, cx: &App) -> Div {
    let mut content = div()
        .v_flex()
        .gap_1()
        .p_3()
        .rounded_md()
        .bg(cx.theme().secondary)
        .font_family("Menlo")
        .text_xs();
    for line in text.lines().take(120) {
        content = content.child(line.to_string());
    }
    if text.lines().count() > 120 {
        content = content.child("… preview limited to 120 lines; copy the complete evidence JSON.");
    }
    content
}

fn color(value: u32) -> Hsla {
    rgb(value).into()
}
