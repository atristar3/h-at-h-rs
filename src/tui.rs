//! Terminal User Interface (TUI) module
//!
//! Provides a real-time dashboard for monitoring the H@H client status,
//! cache statistics, bandwidth usage, and download progress.

use crate::cache::CacheStats;
use crate::metrics::{HahMetrics, format_bytes, format_duration};
use crate::throttle::BandwidthStats;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Row, Sparkline, Table, Tabs},
};
use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Maximum number of data points to keep for graphs
const MAX_HISTORY_POINTS: usize = 60;

/// TUI refresh rate in milliseconds
const REFRESH_RATE_MS: u64 = 250;

/// Data shared with the TUI from the main application
#[derive(Debug, Clone, Default)]
pub struct TuiData {
    pub client_id: String,
    pub client_name: String,
    pub connected: bool,
    pub host: String,
    pub port: u16,
    pub metrics: HahMetrics,
    pub cache_stats: CacheStats,
    pub bandwidth: BandwidthStats,
    pub gallery_downloads: Vec<(String, String, i64, i64)>, // (id, status, downloaded, total)
    pub archive_downloads: Vec<(String, String, String, f64)>, // (id, title, status, progress)
    pub log_messages: Vec<String>,
    pub static_ranges_count: usize,
    pub static_ranges_enabled: bool,
    pub proxy_mode_enabled: bool,
}

/// TUI state for tracking history and UI state
struct TuiState {
    /// Bandwidth history for sparkline (bytes per second)
    bps_history: VecDeque<u64>,
    /// Selected tab
    selected_tab: usize,
    /// Scroll offset for log view
    log_scroll: usize,
    /// Whether to show help overlay
    show_help: bool,
    /// Start time for calculating runtime
    start_time: Instant,
}

impl Default for TuiState {
    fn default() -> Self {
        Self {
            bps_history: VecDeque::with_capacity(MAX_HISTORY_POINTS),
            selected_tab: 0,
            log_scroll: 0,
            show_help: false,
            start_time: Instant::now(),
        }
    }
}

/// Run the TUI application
pub async fn run_tui(
    data_rx: watch::Receiver<TuiData>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut state = TuiState::default();
    let mut last_draw = Instant::now();

    loop {
        // Check for input events (non-blocking)
        if event::poll(Duration::from_millis(REFRESH_RATE_MS))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            // Signal shutdown
                            let _ = shutdown_tx.send(());
                            break;
                        }
                        KeyCode::Char('?') | KeyCode::Char('h') => {
                            state.show_help = !state.show_help;
                        }
                        KeyCode::Tab | KeyCode::Right => {
                            state.selected_tab = (state.selected_tab + 1) % 4;
                        }
                        KeyCode::BackTab | KeyCode::Left => {
                            state.selected_tab = if state.selected_tab == 0 {
                                3
                            } else {
                                state.selected_tab - 1
                            };
                        }
                        KeyCode::Char('1') => state.selected_tab = 0,
                        KeyCode::Char('2') => state.selected_tab = 1,
                        KeyCode::Char('3') => state.selected_tab = 2,
                        KeyCode::Char('4') => state.selected_tab = 3,
                        KeyCode::Up | KeyCode::Char('k') => {
                            state.log_scroll = state.log_scroll.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            state.log_scroll = state.log_scroll.saturating_add(1);
                        }
                        KeyCode::PageUp => {
                            state.log_scroll = state.log_scroll.saturating_sub(10);
                        }
                        KeyCode::PageDown => {
                            state.log_scroll = state.log_scroll.saturating_add(10);
                        }
                        KeyCode::Home => {
                            state.log_scroll = 0;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Get latest data
        let data = data_rx.borrow().clone();

        // Update bandwidth history
        if last_draw.elapsed() >= Duration::from_secs(1) {
            state.bps_history.push_back(data.bandwidth.bytes_per_second);
            if state.bps_history.len() > MAX_HISTORY_POINTS {
                state.bps_history.pop_front();
            }
            last_draw = Instant::now();
        }

        // Draw UI
        terminal.draw(|frame| {
            draw_ui(frame, &data, &mut state);
        })?;
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

/// Main UI drawing function
fn draw_ui(frame: &mut Frame, data: &TuiData, state: &mut TuiState) {
    let size = frame.area();

    // Create main layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // Tabs
            Constraint::Min(10),   // Main content
            Constraint::Length(1), // Footer
        ])
        .split(size);

    // Draw header
    draw_header(frame, chunks[0], data, state);

    // Draw tabs
    draw_tabs(frame, chunks[1], state.selected_tab);

    // Draw main content based on selected tab
    match state.selected_tab {
        0 => draw_dashboard(frame, chunks[2], data, state),
        1 => draw_downloads(frame, chunks[2], data),
        2 => draw_cache(frame, chunks[2], data),
        3 => draw_logs(frame, chunks[2], data, state),
        _ => {}
    }

    // Draw footer
    draw_footer(frame, chunks[3]);

    // Draw help overlay if needed
    if state.show_help {
        draw_help_overlay(frame, size);
    }
}

/// Draw the header with connection status
fn draw_header(frame: &mut Frame, area: Rect, data: &TuiData, state: &TuiState) {
    let status_color = if data.connected {
        Color::Green
    } else {
        Color::Red
    };

    let status_text = if data.connected {
        "● CONNECTED"
    } else {
        "○ OFFLINE"
    };

    let runtime = format_duration(state.start_time.elapsed().as_secs());

    let header_text = vec![Line::from(vec![
        Span::styled("H@H-rs ", Style::default().fg(Color::Cyan).bold()),
        Span::styled(
            format!("v{}", env!("CARGO_PKG_VERSION")),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw("  │  "),
        Span::styled(status_text, Style::default().fg(status_color).bold()),
        Span::raw("  │  "),
        Span::styled(
            format!("Client: {}", data.client_name),
            Style::default().fg(Color::White),
        ),
        Span::raw("  │  "),
        Span::styled(
            format!("Uptime: {}", runtime),
            Style::default().fg(Color::Yellow),
        ),
    ])];

    let header = Paragraph::new(header_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" Hentai@Home Rust Client "),
        )
        .style(Style::default());

    frame.render_widget(header, area);
}

/// Draw the tab bar
fn draw_tabs(frame: &mut Frame, area: Rect, selected: usize) {
    let titles = vec!["Dashboard", "Downloads", "Cache", "Logs"];
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL))
        .select(selected)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("|");

    frame.render_widget(tabs, area);
}

/// Draw the main dashboard view
fn draw_dashboard(frame: &mut Frame, area: Rect, data: &TuiData, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // Status
            Constraint::Length(8), // Metrics
            Constraint::Min(6),    // Bandwidth graph
        ])
        .split(chunks[0]);

    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // Cache stats
            Constraint::Min(6),     // Trust/Quality gauges
        ])
        .split(chunks[1]);

    // Status panel
    draw_status_panel(frame, left_chunks[0], data);

    // Metrics panel
    draw_metrics_panel(frame, left_chunks[1], data);

    // Bandwidth sparkline
    draw_bandwidth_graph(frame, left_chunks[2], state);

    // Cache stats
    draw_cache_stats(frame, right_chunks[0], data);

    // Trust/Quality gauges
    draw_trust_quality(frame, right_chunks[1], data);
}

/// Draw the status panel
fn draw_status_panel(frame: &mut Frame, area: Rect, data: &TuiData) {
    let items = vec![
        format!("Client ID:     {}", data.client_id),
        format!("Server:        {}:{}", data.host, data.port),
        format!(
            "Static Ranges: {} ({})",
            data.static_ranges_count,
            if data.static_ranges_enabled {
                "enabled"
            } else {
                "disabled"
            }
        ),
        format!(
            "Proxy Mode:    {}",
            if data.proxy_mode_enabled {
                "enabled"
            } else {
                "disabled"
            }
        ),
        format!(
            "Connections:   {} (peak: {})",
            data.metrics.current_connections, data.metrics.peak_connections
        ),
    ];

    let text: Vec<Line> = items
        .into_iter()
        .map(|s| Line::from(Span::raw(s)))
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Status ")
        .border_style(Style::default().fg(Color::Blue));

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

/// Draw the metrics panel
fn draw_metrics_panel(frame: &mut Frame, area: Rect, data: &TuiData) {
    let success_rate = if data.metrics.total_requests > 0 {
        (data.metrics.successful_serves as f64 / data.metrics.total_requests as f64) * 100.0
    } else {
        100.0
    };

    let items = vec![
        format!("Requests:      {}", data.metrics.total_requests),
        format!("Success Rate:  {:.1}%", success_rate),
        format!("Bytes Served:  {}", format_bytes(data.metrics.bytes_served)),
        format!("Avg Response:  {:.1}ms", data.metrics.avg_response_time_ms),
        format!(
            "Heartbeats:    {}/{}",
            data.metrics.heartbeats_success, data.metrics.heartbeats_sent
        ),
    ];

    let text: Vec<Line> = items
        .into_iter()
        .map(|s| Line::from(Span::raw(s)))
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Performance ")
        .border_style(Style::default().fg(Color::Magenta));

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

/// Draw the bandwidth sparkline graph
fn draw_bandwidth_graph(frame: &mut Frame, area: Rect, state: &TuiState) {
    let data: Vec<u64> = state.bps_history.iter().copied().collect();

    let max_bps = data.iter().copied().max().unwrap_or(1);
    let current_bps = data.last().copied().unwrap_or(0);

    let title = format!(
        " Bandwidth: {} (max: {}) ",
        format_bytes(current_bps) + "/s",
        format_bytes(max_bps) + "/s"
    );

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(Style::default().fg(Color::Green)),
        )
        .data(&data)
        .style(Style::default().fg(Color::Green));

    frame.render_widget(sparkline, area);
}

/// Draw cache statistics
fn draw_cache_stats(frame: &mut Frame, area: Rect, data: &TuiData) {
    let hit_rate = if data.cache_stats.hits + data.cache_stats.misses > 0 {
        (data.cache_stats.hits as f64 / (data.cache_stats.hits + data.cache_stats.misses) as f64)
            * 100.0
    } else {
        100.0
    };

    let items = vec![
        format!("Total Files:   {}", data.cache_stats.total_files),
        format!(
            "Cache Size:    {}",
            format_bytes(data.cache_stats.total_size)
        ),
        format!("Cache Hits:    {}", data.cache_stats.hits),
        format!("Cache Misses:  {}", data.cache_stats.misses),
        format!("Hit Rate:      {:.1}%", hit_rate),
        format!(
            "Bytes Served:  {}",
            format_bytes(data.cache_stats.bytes_served)
        ),
    ];

    let text: Vec<Line> = items
        .into_iter()
        .map(|s| Line::from(Span::raw(s)))
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Cache ")
        .border_style(Style::default().fg(Color::Yellow));

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

/// Draw trust and quality gauges
fn draw_trust_quality(frame: &mut Frame, area: Rect, data: &TuiData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Trust gauge
    let trust_pct = (data.metrics.trust * 100.0) as u16;
    let trust_color = if trust_pct >= 90 {
        Color::Green
    } else if trust_pct >= 70 {
        Color::Yellow
    } else {
        Color::Red
    };

    let trust_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Trust ")
                .border_style(Style::default().fg(trust_color)),
        )
        .gauge_style(Style::default().fg(trust_color))
        .percent(trust_pct)
        .label(format!("{:.1}%", data.metrics.trust * 100.0));

    frame.render_widget(trust_gauge, chunks[0]);

    // Quality gauge
    let quality_pct = (data.metrics.quality * 100.0) as u16;
    let quality_color = if quality_pct >= 90 {
        Color::Green
    } else if quality_pct >= 70 {
        Color::Yellow
    } else {
        Color::Red
    };

    let quality_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Quality ")
                .border_style(Style::default().fg(quality_color)),
        )
        .gauge_style(Style::default().fg(quality_color))
        .percent(quality_pct)
        .label(format!("{:.1}%", data.metrics.quality * 100.0));

    frame.render_widget(quality_gauge, chunks[1]);
}

/// Draw the downloads view
fn draw_downloads(frame: &mut Frame, area: Rect, data: &TuiData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Gallery downloads
    let gallery_rows: Vec<Row> = data
        .gallery_downloads
        .iter()
        .map(|(id, status, downloaded, total)| {
            let progress = if *total > 0 {
                format!("{:.1}%", (*downloaded as f64 / *total as f64) * 100.0)
            } else {
                "0%".to_string()
            };
            let status_style = match status.as_str() {
                "completed" => Style::default().fg(Color::Green),
                "downloading" => Style::default().fg(Color::Cyan),
                "failed" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::Yellow),
            };
            Row::new(vec![
                id.clone(),
                status.clone(),
                format!("{}/{}", downloaded, total),
                progress,
            ])
            .style(status_style)
        })
        .collect();

    let gallery_table = Table::new(
        gallery_rows,
        [
            Constraint::Percentage(30),
            Constraint::Percentage(20),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ],
    )
    .header(
        Row::new(vec!["Gallery ID", "Status", "Progress", "%"])
            .style(Style::default().fg(Color::Cyan).bold()),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Gallery Downloads ")
            .border_style(Style::default().fg(Color::Blue)),
    );

    frame.render_widget(gallery_table, chunks[0]);

    // Archive downloads
    let archive_rows: Vec<Row> = data
        .archive_downloads
        .iter()
        .map(|(id, title, status, progress)| {
            let display_title = if title.len() > 30 {
                format!("{}...", &title[..27])
            } else {
                title.clone()
            };
            let status_style = match status.as_str() {
                "completed" => Style::default().fg(Color::Green),
                "downloading" => Style::default().fg(Color::Cyan),
                "failed" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::Yellow),
            };
            Row::new(vec![
                id.clone(),
                display_title,
                status.clone(),
                format!("{:.1}%", progress),
            ])
            .style(status_style)
        })
        .collect();

    let archive_table = Table::new(
        archive_rows,
        [
            Constraint::Percentage(20),
            Constraint::Percentage(40),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
        ],
    )
    .header(
        Row::new(vec!["ID", "Title", "Status", "Progress"])
            .style(Style::default().fg(Color::Cyan).bold()),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Archive Downloads ")
            .border_style(Style::default().fg(Color::Magenta)),
    );

    frame.render_widget(archive_table, chunks[1]);
}

/// Draw the cache view with more details
fn draw_cache(frame: &mut Frame, area: Rect, data: &TuiData) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Cache statistics table
    let stats = vec![
        ("Total Files", format!("{}", data.cache_stats.total_files)),
        ("Total Size", format_bytes(data.cache_stats.total_size)),
        ("Cache Hits", format!("{}", data.cache_stats.hits)),
        ("Cache Misses", format!("{}", data.cache_stats.misses)),
        (
            "Hit Rate",
            format!(
                "{:.2}%",
                if data.cache_stats.hits + data.cache_stats.misses > 0 {
                    (data.cache_stats.hits as f64
                        / (data.cache_stats.hits + data.cache_stats.misses) as f64)
                        * 100.0
                } else {
                    100.0
                }
            ),
        ),
        ("Bytes Served", format_bytes(data.cache_stats.bytes_served)),
    ];

    let rows: Vec<Row> = stats
        .into_iter()
        .map(|(label, value)| {
            Row::new(vec![
                Span::styled(label, Style::default().fg(Color::Gray)),
                Span::styled(value, Style::default().fg(Color::White).bold()),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [Constraint::Percentage(50), Constraint::Percentage(50)],
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Cache Statistics ")
            .border_style(Style::default().fg(Color::Yellow)),
    );

    frame.render_widget(table, chunks[0]);

    // Bandwidth statistics
    let bw = &data.bandwidth;
    let bw_stats = vec![
        (
            "Current Speed",
            format!("{}/s", format_bytes(bw.bytes_per_second)),
        ),
        ("Per Minute", format_bytes(bw.bytes_per_minute)),
        ("Per Hour", format_bytes(bw.bytes_per_hour)),
        ("Total Transferred", format_bytes(bw.total_bytes)),
        (
            "Average Speed",
            format!("{:.1} KB/s", bw.average_speed / 1024.0),
        ),
        ("Peak Speed", format!("{}/s", format_bytes(bw.peak_speed))),
        ("Requests Served", format!("{}", bw.requests_served)),
    ];

    let bw_rows: Vec<Row> = bw_stats
        .into_iter()
        .map(|(label, value)| {
            Row::new(vec![
                Span::styled(label, Style::default().fg(Color::Gray)),
                Span::styled(value, Style::default().fg(Color::Green).bold()),
            ])
        })
        .collect();

    let bw_table = Table::new(
        bw_rows,
        [Constraint::Percentage(50), Constraint::Percentage(50)],
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Bandwidth ")
            .border_style(Style::default().fg(Color::Green)),
    );

    frame.render_widget(bw_table, chunks[1]);
}

/// Draw the logs view
fn draw_logs(frame: &mut Frame, area: Rect, data: &TuiData, state: &mut TuiState) {
    let max_scroll = data
        .log_messages
        .len()
        .saturating_sub(area.height as usize - 2);
    state.log_scroll = state.log_scroll.min(max_scroll);

    let visible_logs: Vec<ListItem> = data
        .log_messages
        .iter()
        .skip(state.log_scroll)
        .take(area.height as usize - 2)
        .map(|msg| {
            let style = if msg.contains("ERROR") || msg.contains("error") {
                Style::default().fg(Color::Red)
            } else if msg.contains("WARN") || msg.contains("warn") {
                Style::default().fg(Color::Yellow)
            } else if msg.contains("INFO") || msg.contains("info") {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            };
            ListItem::new(Span::styled(msg.clone(), style))
        })
        .collect();

    let list = List::new(visible_logs).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Logs [{}/{}] ",
                state.log_scroll + 1,
                data.log_messages.len().max(1)
            ))
            .border_style(Style::default().fg(Color::White)),
    );

    frame.render_widget(list, area);
}

/// Draw the footer with keyboard shortcuts
fn draw_footer(frame: &mut Frame, area: Rect) {
    let text = Line::from(vec![
        Span::styled(" q ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" Quit  "),
        Span::styled(" Tab ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" Switch Tab  "),
        Span::styled(" 1-4 ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" Select Tab  "),
        Span::styled(" ? ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" Help  "),
        Span::styled(" ↑↓ ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" Scroll "),
    ]);

    let paragraph =
        Paragraph::new(text).style(Style::default().fg(Color::White).bg(Color::DarkGray));

    frame.render_widget(paragraph, area);
}

/// Draw help overlay
fn draw_help_overlay(frame: &mut Frame, area: Rect) {
    // Calculate centered popup area
    let popup_width = 50;
    let popup_height = 15;
    let popup_x = (area.width.saturating_sub(popup_width)) / 2;
    let popup_y = (area.height.saturating_sub(popup_height)) / 2;

    let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

    // Clear the popup area
    let clear = Block::default().style(Style::default().bg(Color::DarkGray));
    frame.render_widget(clear, popup_area);

    let help_text = vec![
        Line::from(Span::styled(
            "Keyboard Shortcuts",
            Style::default().fg(Color::Cyan).bold(),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("  q, Esc    ", Style::default().fg(Color::Yellow)),
            Span::raw("Quit application"),
        ]),
        Line::from(vec![
            Span::styled("  Tab       ", Style::default().fg(Color::Yellow)),
            Span::raw("Next tab"),
        ]),
        Line::from(vec![
            Span::styled("  Shift+Tab ", Style::default().fg(Color::Yellow)),
            Span::raw("Previous tab"),
        ]),
        Line::from(vec![
            Span::styled("  1-4       ", Style::default().fg(Color::Yellow)),
            Span::raw("Jump to tab"),
        ]),
        Line::from(vec![
            Span::styled("  ↑/k       ", Style::default().fg(Color::Yellow)),
            Span::raw("Scroll up"),
        ]),
        Line::from(vec![
            Span::styled("  ↓/j       ", Style::default().fg(Color::Yellow)),
            Span::raw("Scroll down"),
        ]),
        Line::from(vec![
            Span::styled("  PgUp/PgDn ", Style::default().fg(Color::Yellow)),
            Span::raw("Page scroll"),
        ]),
        Line::from(vec![
            Span::styled("  ?/h       ", Style::default().fg(Color::Yellow)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let help = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Help ")
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .style(Style::default().bg(Color::DarkGray));

    frame.render_widget(help, popup_area);
}

/// Create the TUI data sender/receiver pair
pub fn create_tui_channel() -> (watch::Sender<TuiData>, watch::Receiver<TuiData>) {
    watch::channel(TuiData::default())
}
