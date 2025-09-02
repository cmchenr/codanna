//! CLI entry point for the codebase intelligence system.
//!
//! Provides commands for indexing, querying, and serving code intelligence data.
//! Main components: Cli parser, Commands enum, and async runtime with MCP server support.

use clap::{
    Parser, Subcommand,
    builder::styling::{AnsiColor, Effects, Styles},
};
use codanna::FileId;
use codanna::parsing::{
    GoParser, LanguageParser, PhpParser, PythonParser, RustParser, TypeScriptParser,
};
use codanna::types::SymbolCounter;
use codanna::{IndexPersistence, Settings, SimpleIndexer, Symbol, SymbolKind};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

// MCP tool JSON output structures
#[derive(Debug, Serialize)]
struct IndexInfo {
    symbol_count: usize,
    file_count: usize,
    relationship_count: usize,
    symbol_kinds: SymbolKindBreakdown,
    semantic_search: SemanticSearchInfo,
}

#[derive(Debug, Serialize)]
struct SymbolKindBreakdown {
    functions: usize,
    methods: usize,
    structs: usize,
    traits: usize,
}

#[derive(Debug, Serialize)]
struct SemanticSearchInfo {
    enabled: bool,
    model_name: Option<String>,
    embeddings: Option<usize>,
    dimensions: Option<usize>,
    created: Option<String>,
    updated: Option<String>,
}

fn clap_cargo_style() -> Styles {
    Styles::styled()
        .header(AnsiColor::Cyan.on_default() | Effects::BOLD)
        .usage(AnsiColor::Cyan.on_default() | Effects::BOLD)
        .literal(AnsiColor::Green.on_default())
        .placeholder(AnsiColor::Green.on_default())
}

/// Create custom help text with consistent styling
fn create_custom_help() -> String {
    use codanna::display::theme::Theme;
    use console::style;

    let mut help = String::new();

    // Quick Start section
    if Theme::should_disable_colors() {
        help.push_str("Quick Start:\n");
    } else {
        help.push_str(&format!("{}\n", style("Quick Start:").cyan().bold()));
    }
    help.push_str("  $ codanna init                   # Initialize in current directory\n");
    help.push_str("  $ codanna index src              # Index your source code\n");
    help.push_str("  $ codanna serve --http --watch   # HTTP server with OAuth\n");
    help.push_str("  $ codanna serve --https --watch  # HTTPS server with TLS\n\n");

    // About section
    help.push_str("Index code and query relationships, symbols, and dependencies.\n\n");

    // Usage
    if Theme::should_disable_colors() {
        help.push_str("Usage:");
    } else {
        help.push_str(&format!("{}", style("Usage:").cyan().bold()));
    }
    help.push_str(" codanna [OPTIONS] <COMMAND>\n\n");

    // Commands
    if Theme::should_disable_colors() {
        help.push_str("Commands:\n");
    } else {
        help.push_str(&format!("{}\n", style("Commands:").cyan().bold()));
    }
    help.push_str("  init        Set up .codanna directory\n");
    help.push_str("  index       Build searchable index from codebase\n");
    help.push_str("  retrieve    Query symbols, relationships, and dependencies\n");
    help.push_str("  serve       Start MCP server\n");
    help.push_str("  config      Display active settings\n");
    help.push_str("  mcp-test    Test MCP connection\n");
    help.push_str("  mcp         Execute MCP tools directly\n");
    help.push_str("  benchmark   Benchmark parser performance\n");
    help.push_str("  help        Print this message or the help of the given subcommand(s)\n\n");

    help.push_str("See 'codanna help <command>' for more information on a specific command.\n\n");

    // Options
    if Theme::should_disable_colors() {
        help.push_str("Options:\n");
    } else {
        help.push_str(&format!("{}\n", style("Options:").cyan().bold()));
    }
    help.push_str("  -c, --config <CONFIG>  Path to custom settings.toml file\n");
    help.push_str("      --info             Show detailed loading information\n");
    help.push_str("  -h, --help             Print help\n");
    help.push_str("  -V, --version          Print version\n\n");

    // Learn More
    if Theme::should_disable_colors() {
        help.push_str("Learn More:\n");
    } else {
        help.push_str(&format!("{}\n", style("Learn More:").cyan().bold()));
    }
    help.push_str("  GitHub: https://github.com/bartolli/codanna");

    help
}

/// Code intelligence system
#[derive(Parser)]
#[command(
    name = "codanna",
    version = env!("CARGO_PKG_VERSION"),
    about = "Code intelligence system",
    long_about = "Index code and query relationships, symbols, and dependencies.",
    next_line_help = true,
    styles = clap_cargo_style(),
    override_help = create_custom_help()
)]
struct Cli {
    /// Path to custom settings.toml file
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Show detailed loading information
    #[arg(long, global = true)]
    info: bool,

    #[command(subcommand)]
    command: Commands,
}

/// Available CLI commands
#[derive(Subcommand)]
enum Commands {
    /// Initialize project
    #[command(about = "Set up .codanna directory with default configuration")]
    Init {
        /// Force overwrite existing configuration
        #[arg(short, long)]
        force: bool,
    },

    /// Index source files or directories
    #[command(about = "Build searchable index from codebase")]
    Index {
        /// Path to file or directory to index
        path: PathBuf,

        /// Number of threads to use (overrides config)
        #[arg(short, long)]
        threads: Option<usize>,

        /// Force re-indexing even if index exists
        #[arg(short, long)]
        force: bool,

        /// Show progress during indexing
        #[arg(short, long)]
        progress: bool,

        /// Dry run - show what would be indexed without indexing
        #[arg(long)]
        dry_run: bool,

        /// Maximum number of files to index
        #[arg(long)]
        max_files: Option<usize>,
    },

    /// Query code relationships and dependencies
    #[command(
        about = "Search symbols, find callers/callees, analyze impact",
        long_about = "Query indexed symbols, relationships, and dependencies.",
        after_help = "Examples:\n  codanna retrieve symbol main\n  codanna retrieve callers process_file\n  codanna retrieve calls init\n  codanna retrieve implementations Parser\n  codanna retrieve describe OutputManager\n  codanna retrieve search \"parse\" --limit 10\n\nJSON paths:\n  retrieve symbol     .data.items[0].symbol.name\n  retrieve search     .data.items[].symbol.name\n  retrieve callers    .data.items[].symbol.name\n  retrieve describe   .data.items[0].symbol.name"
    )]
    Retrieve {
        #[command(subcommand)]
        query: RetrieveQuery,
    },

    /// Show current configuration settings
    #[command(about = "Display active settings from .codanna/settings.toml")]
    Config,

    /// Start MCP server
    #[command(
        about = "Start MCP server",
        long_about = "Start MCP server with optional HTTP/HTTPS modes.",
        after_help = "Examples:\n  codanna serve\n  codanna serve --http --watch\n  codanna serve --https --watch\n  codanna serve --http --bind 0.0.0.0:3000\n\nModes:\n  Default: stdio\n  --http: HTTP with OAuth\n  --https: HTTPS with TLS"
    )]
    Serve {
        /// Watch index file for changes and auto-reload
        #[arg(long, help = "Enable hot-reload when index changes")]
        watch: bool,

        /// Check interval in seconds (default: 5)
        #[arg(
            long,
            default_value = "5",
            help = "How often to check for index changes"
        )]
        watch_interval: u64,

        /// Enable HTTP server mode instead of stdio
        #[arg(long, help = "Run as HTTP server instead of stdio transport")]
        http: bool,

        /// Enable HTTPS server mode with TLS
        #[arg(
            long,
            conflicts_with = "http",
            help = "Run as HTTPS server with TLS support"
        )]
        https: bool,

        /// Bind address for HTTP/HTTPS server
        #[arg(
            long,
            default_value = "127.0.0.1:8080",
            help = "Address to bind HTTP/HTTPS server to"
        )]
        bind: String,
    },

    /// Test MCP connection
    #[command(name = "mcp-test", about = "Test MCP connection and list tools")]
    McpTest {
        /// Path to server binary (defaults to current binary)
        #[arg(long)]
        server_binary: Option<PathBuf>,

        /// Tool to call (if not specified, just lists tools)
        #[arg(long)]
        tool: Option<String>,

        /// Tool arguments as JSON
        #[arg(long)]
        args: Option<String>,
    },

    /// Call MCP tools directly (advanced)
    #[command(
        about = "Execute MCP tools directly",
        long_about = "Execute MCP tools directly without spawning a server.\n\nSupports positional arguments, key=value pairs, and JSON arguments.",
        after_help = "Examples:\n  codanna mcp find_symbol main\n  codanna mcp get_calls process_file\n  codanna mcp semantic_search_docs query:\"error handling\" limit:5\n  codanna mcp search_symbols query:parse kind:function\n  codanna mcp find_symbol Parser --json | jq '.data[].symbol.name'\n  codanna mcp search_symbols query:Parser --json | jq '.data[].name'\n\nTools:\n  find_symbol                  Find symbol by exact name\n  search_symbols               Full-text search with fuzzy matching\n  semantic_search_docs         Natural language search\n  semantic_search_with_context Natural language search with relationships\n  get_calls                    Functions called by a function\n  find_callers                 Functions that call a function\n  analyze_impact               Impact radius of symbol changes\n  get_index_info               Index statistics"
    )]
    Mcp {
        /// Tool to call
        tool: String,

        /// Positional arguments (can be simple values or key:value pairs)
        #[arg(num_args = 0..)]
        positional: Vec<String>,

        /// Tool arguments as JSON (for backward compatibility and complex cases)
        #[arg(long)]
        args: Option<String>,

        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Benchmark parser performance
    #[command(about = "Benchmark parser performance")]
    Benchmark {
        /// Language to benchmark (rust, python, all)
        #[arg(default_value = "all")]
        language: String,

        /// Custom file to benchmark
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
}

/// Query types for retrieving indexed information.
///
/// Supports symbol lookups, relationship queries, impact analysis, and full-text search.
#[derive(Subcommand)]
enum RetrieveQuery {
    /// Find a symbol by name
    #[command(
        after_help = "Examples:\n  codanna retrieve symbol main\n  codanna retrieve symbol name:main --json\n  codanna retrieve symbol MyStruct --json | jq '.file'"
    )]
    Symbol {
        /// Positional arguments (symbol name and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,
        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show what functions a given function calls
    #[command(
        after_help = "Examples:\n  codanna retrieve calls process_file\n  codanna retrieve calls function:process_file --json"
    )]
    Calls {
        /// Positional arguments (function name and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,
        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show what functions call a given function
    #[command(
        after_help = "Examples:\n  codanna retrieve callers main\n  codanna retrieve callers function:main --json\n  codanna retrieve callers init --json | jq -r '.[].name'"
    )]
    Callers {
        /// Positional arguments (function name and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,
        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show what types implement a given trait
    #[command(
        after_help = "Examples:\n  codanna retrieve implementations Parser\n  codanna retrieve implementations trait:Parser --json"
    )]
    Implementations {
        /// Positional arguments (trait name and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,
        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show what types a given symbol uses
    Uses {
        /// Name of the symbol
        symbol: String,
    },

    // DISABLED: Impact command has been deprecated in favor of:
    // 1. MCP semantic_search_with_context for comprehensive analysis
    // 2. Slash commands that compose simple retrieve commands
    // 3. Enhanced describe command (future)
    //
    // The impact command had fundamental issues:
    // - Didn't work for non-function symbols (structs, traits, enums)
    // - Returned empty results for valid symbols
    // - Conceptually flawed (not all symbols have "impact" in the same way)
    //
    // /// Show the impact radius of changing a symbol
    // #[command(
    //     after_help = "Examples:\n  codanna retrieve impact MyStruct\n  codanna retrieve impact symbol:MyStruct depth:3\n  codanna retrieve impact main --depth 2 --json"
    // )]
    // Impact {
    //     /// Positional arguments (symbol name and/or key:value pairs)
    //     #[arg(num_args = 0..)]
    //     args: Vec<String>,
    //     /// Maximum depth to search (flag format)
    //     #[arg(short, long)]
    //     depth: Option<usize>,
    //     /// Output in JSON format
    //     #[arg(long)]
    //     json: bool,
    // },
    /// Search for symbols using full-text search
    #[command(
        after_help = "Examples:\n  # Traditional flag format\n  codanna retrieve search \"parse\" --limit 5 --kind function\n  \n  # Key:value format (Unix-style)\n  codanna retrieve search query:parse limit:5 kind:function\n  \n  # Mixed format\n  codanna retrieve search \"parse\" limit:5 --json"
    )]
    Search {
        /// Positional arguments (query and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,

        /// Maximum number of results (flag format)
        #[arg(short, long)]
        limit: Option<usize>,

        /// Filter by symbol kind (flag format)
        #[arg(short, long)]
        kind: Option<String>,

        /// Filter by module path (flag format)
        #[arg(short, long)]
        module: Option<String>,

        /// Output in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Show what methods a type or trait defines
    Defines {
        /// Name of the type or trait
        symbol: String,
    },

    /// Show dependency analysis for a symbol
    Dependencies {
        /// Name of the symbol
        symbol: String,
    },

    /// Show information about a symbol
    #[command(
        after_help = "Examples:\n  codanna retrieve describe SimpleIndexer\n  codanna retrieve describe symbol:SimpleIndexer --json"
    )]
    Describe {
        /// Positional arguments (symbol name and/or key:value pairs)
        #[arg(num_args = 0..)]
        args: Vec<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

/// Entry point with tokio async runtime.
///
/// Handles config initialization, index loading/creation, and command dispatch.
/// Auto-initializes config for index command. Persists index after modifications.
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // For index command, auto-initialize if needed
    if matches!(cli.command, Commands::Index { .. }) {
        if Settings::check_init().is_err() {
            // Auto-initialize for index command
            eprintln!("Initializing project configuration...");
            match Settings::init_config_file(false) {
                Ok(path) => {
                    eprintln!("Created configuration file at: {}", path.display());
                }
                Err(e) => {
                    eprintln!("Warning: Could not create config file: {e}");
                    eprintln!("Using default configuration.");
                }
            }
        }
    } else if !matches!(cli.command, Commands::Init { .. }) {
        // For other commands, just warn
        if let Err(warning) = Settings::check_init() {
            eprintln!("Warning: {warning}");
            eprintln!("Using default configuration for now.");
        }
    }

    // Load configuration
    let mut config = if let Some(config_path) = &cli.config {
        Settings::load_from(config_path).unwrap_or_else(|e| {
            eprintln!(
                "Configuration error loading from {}: {}",
                config_path.display(),
                e
            );
            std::process::exit(1);
        })
    } else {
        Settings::load().unwrap_or_else(|e| {
            eprintln!("Configuration error: {e}");
            Settings::default()
        })
    };

    match &cli.command {
        Commands::Init { force } => {
            let config_path = PathBuf::from(".codanna/settings.toml");

            if config_path.exists() && !force {
                eprintln!(
                    "Configuration file already exists at: {}",
                    config_path.display()
                );
                eprintln!("Use --force to overwrite");
                std::process::exit(1);
            }

            match Settings::init_config_file(*force) {
                Ok(path) => {
                    println!("Created configuration file at: {}", path.display());
                    println!("Edit this file to customize your settings.");
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            return;
        }

        Commands::Config => {
            println!("Current Configuration:");
            println!("{}", "=".repeat(50));
            match toml::to_string_pretty(&config) {
                Ok(toml_str) => println!("{toml_str}"),
                Err(e) => eprintln!("Error displaying config: {e}"),
            }
            return;
        }

        Commands::Index {
            threads: Some(t),
            force: _,
            ..
        } => {
            // Override config with CLI args
            config.indexing.parallel_threads = *t;
        }
        Commands::Index {
            threads: None,
            force: _,
            ..
        } => {
            // Use default from config
        }

        Commands::Serve { .. } => {
            // No configuration overrides for serve
        }

        _ => {}
    }

    // Set up persistence based on config
    let index_path = config.index_path.clone();
    let persistence = IndexPersistence::new(index_path);

    // Skip loading index for mcp-test (thin client mode)
    let skip_index_load = matches!(cli.command, Commands::McpTest { .. });

    // Determine if we need full trait resolver initialization
    // Only needed for trait-related commands: implementations, trait analysis, etc.
    let needs_trait_resolver = matches!(
        cli.command,
        Commands::Retrieve {
            query: RetrieveQuery::Implementations { .. },
            ..
        } | Commands::Index { .. }
            | Commands::Serve { .. }
    );

    // Load existing index or create new one (unless we're in thin client mode)
    let settings = Arc::new(config.clone());
    let mut indexer = if skip_index_load {
        SimpleIndexer::with_settings(settings.clone()) // Empty indexer, won't be used
    } else {
        let force_recreate_index =
            matches!(cli.command, Commands::Index { force: true, ref path, .. } if path.is_dir());
        if persistence.exists() && !force_recreate_index {
            if config.debug {
                eprintln!(
                    "DEBUG: Found existing index at {}",
                    config.index_path.display()
                );
            }
            // Use lazy loading for simple commands to improve startup time
            let skip_trait_resolver = !needs_trait_resolver;
            if skip_trait_resolver && config.debug {
                eprintln!("DEBUG: Using lazy initialization (skipping trait resolver)");
            }

            match persistence.load_with_settings_lazy(
                settings.clone(),
                cli.info,
                skip_trait_resolver,
            ) {
                Ok(loaded) => {
                    if config.debug {
                        eprintln!("DEBUG: Successfully loaded index from disk");
                    }
                    if cli.info {
                        eprintln!(
                            "Loaded existing index (total: {} symbols)",
                            loaded.symbol_count()
                        );
                    }
                    loaded
                }
                Err(e) => {
                    eprintln!("Warning: Could not load index: {e}. Creating new index.");
                    SimpleIndexer::with_settings(settings.clone())
                }
            }
        } else {
            if force_recreate_index && persistence.exists() {
                eprintln!("Force re-indexing requested, creating new index");
            } else if !persistence.exists() && config.debug {
                eprintln!(
                    "DEBUG: No existing index found at {}",
                    config.index_path.display()
                );
            }
            if config.debug {
                eprintln!("DEBUG: Creating new index");
            }
            // Create a new indexer with the given settings
            let mut new_indexer = SimpleIndexer::with_settings_lazy(settings.clone());
            // Clear Tantivy index if force re-indexing directory
            if force_recreate_index {
                if let Err(e) = new_indexer.clear_tantivy_index() {
                    eprintln!("Warning: Failed to clear Tantivy index: {e}");
                }
            }
            new_indexer
        }
    };

    // Enable semantic search if configured
    if config.semantic_search.enabled && !indexer.has_semantic_search() {
        if let Err(e) = indexer.enable_semantic_search() {
            eprintln!("Warning: Failed to enable semantic search: {e}");
        } else {
            eprintln!(
                "Semantic search enabled (model: {}, threshold: {})",
                config.semantic_search.model, config.semantic_search.threshold
            );
        }
    }

    match cli.command {
        Commands::Init { .. } | Commands::Config => {
            // Already handled above
            unreachable!()
        }

        Commands::Serve {
            watch,
            watch_interval,
            http,
            https,
            bind,
        } => {
            // Determine server mode:
            // 1. CLI --https flag takes highest precedence
            // 2. CLI --http flag takes second precedence
            // 3. Otherwise, check config.server.mode
            let server_mode = if https {
                "https"
            } else if http || config.server.mode == "http" {
                "http"
            } else {
                "stdio"
            };

            // Use bind address from CLI if provided, otherwise from config
            // For HTTPS, default to port 8443 if using default bind
            let bind_address = if bind != "127.0.0.1:8080" {
                // CLI flag was explicitly set (not default)
                bind
            } else if https {
                // For HTTPS, use port 8443 by default
                "127.0.0.1:8443".to_string()
            } else {
                // Use config value
                config.server.bind.clone()
            };

            // Use watch interval from CLI if provided, otherwise from config
            let actual_watch_interval = if watch_interval != 5 {
                // CLI flag was explicitly set (not default)
                watch_interval
            } else {
                config.server.watch_interval
            };

            match server_mode {
                "https" => {
                    // HTTPS mode - secure server with TLS
                    if config.mcp.debug {
                        eprintln!("Starting MCP server in HTTPS mode with TLS");
                        eprintln!("Bind address: {bind_address}");
                        if watch || config.file_watch.enabled {
                            eprintln!(
                                "File watching: ENABLED (event-driven with {}ms debounce)",
                                config.file_watch.debounce_ms
                            );
                        }
                    }

                    // Use the HTTPS server implementation
                    #[cfg(feature = "https-server")]
                    {
                        use codanna::mcp::https_server::serve_https;
                        if let Err(e) = serve_https(config, watch, bind_address).await {
                            eprintln!("HTTPS server error: {e}");
                            std::process::exit(1);
                        }
                    }

                    #[cfg(not(feature = "https-server"))]
                    {
                        eprintln!("HTTPS server support is not compiled in.");
                        eprintln!("Please rebuild with: cargo build --features https-server");
                        std::process::exit(1);
                    }
                }
                "http" => {
                    // HTTP mode - persistent server with event-driven file watching
                    eprintln!("Starting MCP server in HTTP mode");
                    eprintln!("Bind address: {bind_address}");
                    if watch || config.file_watch.enabled {
                        eprintln!(
                            "File watching: ENABLED (event-driven with {}ms debounce)",
                            config.file_watch.debounce_ms
                        );
                    }

                    // Use the HTTP server implementation
                    use codanna::mcp::http_server::serve_http;
                    if let Err(e) = serve_http(config, watch, bind_address).await {
                        eprintln!("HTTP server error: {e}");
                        std::process::exit(1);
                    }
                }
                _ => {
                    // stdio mode - current implementation
                    eprintln!("Starting MCP server on stdio transport");
                    if watch {
                        eprintln!("Index watching enabled (interval: {actual_watch_interval}s)");
                    }
                    eprintln!("To test: npx @modelcontextprotocol/inspector cargo run -- serve");

                    // Create MCP server from existing index
                    let server = codanna::mcp::CodeIntelligenceServer::from_persistence(&config)
                        .await
                        .map_err(|e| {
                            eprintln!("Failed to create MCP server: {e}");
                            std::process::exit(1);
                        })
                        .unwrap();

                    // If watch mode is enabled, start the index watcher
                    if watch {
                        use codanna::mcp::watcher::IndexWatcher;
                        use std::time::Duration;

                        let indexer_arc = server.get_indexer_arc();
                        let settings = Arc::new(config.clone());
                        let server_arc = Arc::new(server.clone());
                        let watcher = IndexWatcher::new(
                            indexer_arc,
                            settings,
                            Duration::from_secs(actual_watch_interval),
                        )
                        .with_mcp_server(server_arc);

                        // Spawn watcher in background
                        tokio::spawn(async move {
                            watcher.watch().await;
                        });

                        eprintln!("Index watcher started with notification support");
                    }

                    // If file watching is enabled in config, start the file system watcher
                    if config.file_watch.enabled {
                        use codanna::indexing::FileSystemWatcher;

                        eprintln!("Starting file system watcher for indexed files");
                        eprintln!("  Debounce interval: {}ms", config.file_watch.debounce_ms);

                        let watcher_indexer = server.get_indexer_arc();
                        let watcher = FileSystemWatcher::new(
                            watcher_indexer,
                            config.file_watch.debounce_ms,
                            config.mcp.debug,
                        )
                        .map_err(|e| {
                            eprintln!("Failed to create file system watcher: {e}");
                            eprintln!("File watching disabled for this session");
                            e
                        });

                        if let Ok(watcher) = watcher {
                            // Spawn file watcher in background
                            tokio::spawn(async move {
                                if let Err(e) = watcher.watch().await {
                                    eprintln!("File watcher error: {e}");
                                }
                            });
                            eprintln!(
                                "File system watcher started - monitoring indexed files for changes"
                            );
                        }
                    }

                    // Start server with stdio transport
                    use rmcp::{ServiceExt, transport::stdio};
                    let service = server
                        .serve(stdio())
                        .await
                        .map_err(|e| {
                            eprintln!("Failed to start MCP server: {e}");
                            std::process::exit(1);
                        })
                        .unwrap();

                    // Wait for server to complete
                    service
                        .waiting()
                        .await
                        .map_err(|e| {
                            eprintln!("MCP server error: {e}");
                            std::process::exit(1);
                        })
                        .unwrap();
                } // End of else block for stdio mode
            } // End of match
        }

        Commands::Index {
            path,
            force,
            progress,
            dry_run,
            max_files,
            ..
        } => {
            // Determine if path is a file or directory
            if path.is_file() {
                // Single file indexing
                match indexer.index_file_with_force(&path, force) {
                    Ok(result) => {
                        let language_name = path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .and_then(|ext| {
                                let registry = codanna::parsing::get_registry();
                                registry.lock().ok().and_then(|r| {
                                    r.get_by_extension(ext).map(|def| def.name().to_string())
                                })
                            })
                            .unwrap_or_else(|| "unknown".to_string());

                        if result.is_cached() {
                            println!(
                                "Successfully loaded from cache: {} [{}]",
                                path.display(),
                                language_name
                            );
                        } else {
                            println!(
                                "Successfully indexed: {} [{}]",
                                path.display(),
                                language_name
                            );
                        }
                        println!("File ID: {}", result.file_id().value());

                        // Get symbols for just this file
                        let file_symbols = indexer.get_symbols_by_file(result.file_id());
                        println!("Found {} symbols in this file", file_symbols.len());
                        println!("Total symbols in index: {}", indexer.symbol_count());

                        // Show summary of what was found in this file
                        let functions = file_symbols
                            .iter()
                            .filter(|s| s.kind == SymbolKind::Function)
                            .count();
                        let methods = file_symbols
                            .iter()
                            .filter(|s| s.kind == SymbolKind::Method)
                            .count();
                        let structs = file_symbols
                            .iter()
                            .filter(|s| s.kind == SymbolKind::Struct)
                            .count();
                        let traits = file_symbols
                            .iter()
                            .filter(|s| s.kind == SymbolKind::Trait)
                            .count();

                        println!("  Functions: {functions}");
                        println!("  Methods: {methods}");
                        println!("  Structs: {structs}");
                        println!("  Traits: {traits}");

                        // Save the index
                        if config.debug {
                            eprintln!(
                                "DEBUG: Saving index with {} symbols",
                                indexer.symbol_count()
                            );
                        }
                        match persistence.save(&indexer) {
                            Ok(_) => {
                                println!("\nIndex saved to: {}", config.index_path.display());
                                if config.debug {
                                    eprintln!("DEBUG: Index saved successfully");
                                }
                            }
                            Err(e) => eprintln!("\nWarning: Could not save index: {e}"),
                        }
                    }
                    Err(e) => {
                        eprintln!("Error indexing file: {e}");

                        // Display recovery suggestions
                        let suggestions = e.recovery_suggestions();
                        if !suggestions.is_empty() {
                            eprintln!("\nSuggestions:");
                            for suggestion in suggestions {
                                eprintln!("  • {suggestion}");
                            }
                        }

                        std::process::exit(1);
                    }
                }
            } else if path.is_dir() {
                // Directory indexing
                if let Some(max) = max_files {
                    println!(
                        "Indexing directory: {} (limited to {} files)",
                        path.display(),
                        max
                    );
                } else {
                    println!("Indexing directory: {}", path.display());
                }

                match indexer
                    .index_directory_with_options(&path, progress, dry_run, force, max_files)
                {
                    Ok(stats) => {
                        stats.display();

                        if !dry_run && stats.files_indexed > 0 {
                            // Build symbol cache before saving
                            if let Err(e) = indexer.build_symbol_cache() {
                                eprintln!("Warning: Failed to build symbol cache: {e}");
                            }

                            // Save the index
                            eprintln!(
                                "\nSaving index with {} total symbols, {} total relationships...",
                                indexer.symbol_count(),
                                indexer.relationship_count()
                            );
                            match persistence.save(&indexer) {
                                Ok(_) => {
                                    println!("Index saved to: {}", config.index_path.display());
                                }
                                Err(e) => {
                                    eprintln!("Error: Could not save index: {e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error indexing directory: {e}");

                        // Display recovery suggestions
                        let suggestions = e.recovery_suggestions();
                        if !suggestions.is_empty() {
                            eprintln!("\nSuggestions:");
                            for suggestion in suggestions {
                                eprintln!("  • {suggestion}");
                            }
                        }

                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("Error: Path does not exist: {}", path.display());
                std::process::exit(1);
            }
        }

        Commands::Retrieve { query } => {
            use codanna::io::OutputFormat;
            use codanna::retrieve;

            let exit_code = match query {
                RetrieveQuery::Symbol { args, json } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for symbol name and key:value pairs
                    let (positional_name, params) = parse_positional_args(&args);

                    // Determine symbol name (priority: positional > key:value)
                    let final_name = positional_name
                        .or_else(|| params.get("name").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: symbol requires a name");
                            eprintln!("Usage: codanna retrieve symbol main");
                            eprintln!("   or: codanna retrieve symbol name:main");
                            std::process::exit(1);
                        });

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_symbol(&indexer, &final_name, language, format)
                }
                RetrieveQuery::Callers { args, json } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for function name and key:value pairs
                    let (positional_function, params) = parse_positional_args(&args);

                    // Determine function name (priority: positional > key:value)
                    let final_function = positional_function
                        .or_else(|| params.get("function").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: callers requires a function name");
                            eprintln!("Usage: codanna retrieve callers main");
                            eprintln!("   or: codanna retrieve callers function:main");
                            std::process::exit(1);
                        });

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_callers(&indexer, &final_function, language, format)
                }
                RetrieveQuery::Calls { args, json } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for function name and key:value pairs
                    let (positional_function, params) = parse_positional_args(&args);

                    // Determine function name (priority: positional > key:value)
                    let final_function = positional_function
                        .or_else(|| params.get("function").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: calls requires a function name");
                            eprintln!("Usage: codanna retrieve calls process_file");
                            eprintln!("   or: codanna retrieve calls function:process_file");
                            std::process::exit(1);
                        });

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_calls(&indexer, &final_function, language, format)
                }
                RetrieveQuery::Implementations { args, json } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for trait name and key:value pairs
                    let (positional_trait, params) = parse_positional_args(&args);

                    // Determine trait name (priority: positional > key:value)
                    let final_trait = positional_trait
                        .or_else(|| params.get("trait").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: implementations requires a trait name");
                            eprintln!("Usage: codanna retrieve implementations Parser");
                            eprintln!("   or: codanna retrieve implementations trait:Parser");
                            std::process::exit(1);
                        });

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_implementations(&indexer, &final_trait, language, format)
                }
                RetrieveQuery::Search {
                    args,
                    limit,
                    json,
                    kind,
                    module,
                } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for query and key:value pairs
                    let (positional_query, params) = parse_positional_args(&args);

                    // Determine query source (priority: positional > key:value)
                    let final_query = positional_query
                        .or_else(|| params.get("query").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: search requires a query");
                            eprintln!("Usage: codanna retrieve search \"query\" [options]");
                            eprintln!(
                                "   or: codanna retrieve search query:\"search text\" [options]"
                            );
                            std::process::exit(1);
                        });

                    // Merge parameters (flags take precedence over key:value)
                    let final_limit = limit.unwrap_or_else(|| {
                        params
                            .get("limit")
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(10)
                    });

                    let final_kind = kind.or_else(|| params.get("kind").cloned());
                    let final_module = module.or_else(|| params.get("module").cloned());

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    // Call retrieve function with merged parameters
                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_search(
                        &indexer,
                        &final_query,
                        final_limit,
                        final_kind.as_deref(),
                        final_module.as_deref(),
                        language,
                        format,
                    )
                }
                // DISABLED: Impact command handler commented out
                // See the RetrieveQuery enum for deprecation details
                // RetrieveQuery::Impact { args, depth, json } => {
                //     use codanna::io::args::parse_positional_args;
                //
                //     // Parse positional arguments for symbol name and key:value pairs
                //     let (positional_symbol, params) = parse_positional_args(&args);
                //
                //     // Determine symbol name (priority: positional > key:value)
                //     let final_symbol = positional_symbol
                //         .or_else(|| params.get("symbol").cloned())
                //         .unwrap_or_else(|| {
                //             eprintln!("Error: impact requires a symbol name");
                //             eprintln!("Usage: codanna retrieve impact MyStruct");
                //             eprintln!("   or: codanna retrieve impact symbol:MyStruct depth:3");
                //             std::process::exit(1);
                //         });
                //
                //     // Merge depth parameter (flags take precedence over key:value)
                //     let final_depth = depth.unwrap_or_else(|| {
                //         params
                //             .get("depth")
                //             .and_then(|s| s.parse::<usize>().ok())
                //             .unwrap_or(5)
                //     });
                //
                //     let format = OutputFormat::from_json_flag(json);
                //     retrieve::retrieve_impact(&indexer, &final_symbol, final_depth, format)
                // }
                RetrieveQuery::Describe { args, json } => {
                    use codanna::io::args::parse_positional_args;

                    // Parse positional arguments for symbol name and key:value pairs
                    let (positional_symbol, params) = parse_positional_args(&args);

                    // Determine symbol name (priority: positional > key:value)
                    let final_symbol = positional_symbol
                        .or_else(|| params.get("symbol").cloned())
                        .unwrap_or_else(|| {
                            eprintln!("Error: describe requires a symbol name");
                            eprintln!("Usage: codanna retrieve describe SimpleIndexer");
                            eprintln!("   or: codanna retrieve describe symbol:SimpleIndexer");
                            std::process::exit(1);
                        });

                    // Extract language filter
                    let language = params.get("lang").map(|s| s.as_str());

                    let format = OutputFormat::from_json_flag(json);
                    retrieve::retrieve_describe(&indexer, &final_symbol, language, format)
                }
                RetrieveQuery::Uses { symbol } => {
                    eprintln!("'retrieve uses' command not yet implemented for: {symbol}");
                    codanna::io::ExitCode::GeneralError
                }
                RetrieveQuery::Defines { symbol } => {
                    eprintln!("'retrieve defines' command not yet implemented for: {symbol}");
                    codanna::io::ExitCode::GeneralError
                }
                RetrieveQuery::Dependencies { symbol } => {
                    eprintln!("'retrieve dependencies' command not yet implemented for: {symbol}");
                    codanna::io::ExitCode::GeneralError
                }
            };

            std::process::exit(exit_code as i32);
        }

        Commands::McpTest {
            server_binary,
            tool: _,
            args: _,
        } => {
            use codanna::mcp::client::CodeIntelligenceClient;

            // Get server binary path (default to current executable)
            let server_path = server_binary.unwrap_or_else(|| {
                std::env::current_exe().expect("Failed to get current executable path")
            });

            // Run the test
            if let Err(e) = CodeIntelligenceClient::test_server(server_path).await {
                eprintln!("MCP test failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::Mcp {
            tool,
            positional,
            args,
            json,
        } => {
            use codanna::io::args::parse_positional_args;

            // Build arguments from both positional and --args
            let mut arguments = if let Some(args_str) = &args {
                // Parse JSON arguments if provided (backward compatibility)
                match serde_json::from_str::<serde_json::Value>(args_str) {
                    Ok(serde_json::Value::Object(map)) => Some(map),
                    Ok(_) => {
                        eprintln!("Error: Arguments must be a JSON object");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("Error parsing arguments: {e}");
                        std::process::exit(1);
                    }
                }
            } else {
                // Start with empty map if no --args
                Some(serde_json::Map::new())
            };

            // Process positional arguments using unified parser
            if !positional.is_empty() {
                if let Some(ref mut args_map) = arguments {
                    // Use the unified parser from args.rs
                    let (first_positional, params) = parse_positional_args(&positional);

                    // Handle the first positional argument based on tool type
                    if let Some(pos_arg) = first_positional {
                        match tool.as_str() {
                            "find_symbol" => {
                                args_map.insert(
                                    "name".to_string(),
                                    serde_json::Value::String(pos_arg.clone()),
                                );
                            }
                            "get_calls" | "find_callers" => {
                                args_map.insert(
                                    "function_name".to_string(),
                                    serde_json::Value::String(pos_arg.clone()),
                                );
                            }
                            "analyze_impact" => {
                                args_map.insert(
                                    "symbol_name".to_string(),
                                    serde_json::Value::String(pos_arg.clone()),
                                );
                            }
                            "semantic_search_docs" | "semantic_search_with_context" => {
                                args_map.insert(
                                    "query".to_string(),
                                    serde_json::Value::String(pos_arg.clone()),
                                );
                            }
                            "search_symbols" => {
                                args_map.insert(
                                    "query".to_string(),
                                    serde_json::Value::String(pos_arg.clone()),
                                );
                            }
                            _ => {
                                eprintln!(
                                    "Warning: Unknown tool '{tool}', ignoring positional argument"
                                );
                            }
                        }
                    }

                    // Add all key:value pairs from params
                    for (key, value) in params {
                        // Try to parse as number first, then boolean, fallback to string
                        let json_value = if let Ok(n) = value.parse::<i64>() {
                            serde_json::Value::Number(n.into())
                        } else if let Ok(f) = value.parse::<f64>() {
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(f)
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
                            )
                        } else if let Ok(b) = value.parse::<bool>() {
                            serde_json::Value::Bool(b)
                        } else {
                            serde_json::Value::String(value)
                        };
                        args_map.insert(key, json_value);
                    }
                }
            }

            // Convert to Option<Map> only if we have arguments
            let arguments = arguments.filter(|map| !map.is_empty());

            // Collect data for find_symbol if JSON output is requested
            let find_symbol_data = if json && tool == "find_symbol" {
                let name = arguments
                    .as_ref()
                    .and_then(|m| m.get("name"))
                    .and_then(|v| v.as_str());
                let language = arguments
                    .as_ref()
                    .and_then(|m| m.get("lang"))
                    .and_then(|v| v.as_str());

                if let Some(symbol_name) = name {
                    let symbols = indexer.find_symbols_by_name(symbol_name, language);
                    if !symbols.is_empty() {
                        use codanna::symbol::context::ContextIncludes;
                        let mut results = Vec::new();

                        for symbol in symbols {
                            // Get full context with callers using the same approach as MCP
                            let context = indexer.get_symbol_context(
                                symbol.id,
                                ContextIncludes::CALLERS
                                    | ContextIncludes::IMPLEMENTATIONS
                                    | ContextIncludes::DEFINITIONS,
                            );

                            // Build result with context if available
                            if let Some(ctx) = context {
                                results.push(ctx);
                            } else {
                                // Fallback: create minimal context
                                let file_path = indexer
                                    .get_file_path(symbol.file_id)
                                    .unwrap_or_else(|| "unknown".to_string());

                                results.push(codanna::symbol::context::SymbolContext {
                                    symbol,
                                    file_path,
                                    relationships: Default::default(),
                                });
                            }
                        }
                        Some(results)
                    } else {
                        Some(Vec::new())
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Collect data for get_calls if JSON output is requested
            let get_calls_data = if json && tool == "get_calls" {
                let function_name = arguments
                    .as_ref()
                    .and_then(|m| m.get("function_name"))
                    .and_then(|v| v.as_str());
                let language = arguments
                    .as_ref()
                    .and_then(|m| m.get("lang"))
                    .and_then(|v| v.as_str());

                if let Some(func_name) = function_name {
                    // Find the function first
                    let symbols = indexer.find_symbols_by_name(func_name, language);
                    if let Some(symbol) = symbols.into_iter().find(|s| {
                        matches!(
                            s.kind,
                            crate::SymbolKind::Function | crate::SymbolKind::Method
                        )
                    }) {
                        use codanna::symbol::context::ContextIncludes;
                        // Get context with calls
                        let context = indexer.get_symbol_context(symbol.id, ContextIncludes::CALLS);

                        if let Some(ctx) = context {
                            // Extract just the calls from the context
                            if let Some(calls) = ctx.relationships.calls {
                                Some(calls)
                            } else {
                                Some(Vec::new())
                            }
                        } else {
                            Some(Vec::new())
                        }
                    } else {
                        None // Function not found
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Collect data for find_callers if JSON output is requested
            let find_callers_data = if json && tool == "find_callers" {
                let function_name = arguments
                    .as_ref()
                    .and_then(|m| m.get("function_name"))
                    .and_then(|v| v.as_str());
                let language = arguments
                    .as_ref()
                    .and_then(|m| m.get("lang"))
                    .and_then(|v| v.as_str());

                if let Some(func_name) = function_name {
                    // Find all functions with this name
                    let symbols = indexer.find_symbols_by_name(func_name, language);
                    if !symbols.is_empty() {
                        let mut all_callers = Vec::new();

                        // Check all symbols with this name (could be multiple overloads)
                        for symbol in &symbols {
                            let callers = indexer.get_calling_functions_with_metadata(symbol.id);
                            all_callers.extend(callers);
                        }

                        Some(all_callers)
                    } else {
                        None // Function not found
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Collect data for analyze_impact if JSON output is requested
            let analyze_impact_data = if json && tool == "analyze_impact" {
                let symbol_name = arguments
                    .as_ref()
                    .and_then(|m| m.get("symbol_name"))
                    .and_then(|v| v.as_str());
                let language = arguments
                    .as_ref()
                    .and_then(|m| m.get("lang"))
                    .and_then(|v| v.as_str());

                if let Some(sym_name) = symbol_name {
                    // Find the symbol first
                    let symbols = indexer.find_symbols_by_name(sym_name, language);
                    if let Some(symbol) = symbols.first() {
                        let max_depth = arguments
                            .as_ref()
                            .and_then(|m| m.get("max_depth"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(3) as usize;

                        // Get impact radius - returns Vec<SymbolId>
                        let impacted_ids = indexer.get_impact_radius(symbol.id, Some(max_depth));

                        // Convert SymbolIds to full Symbols
                        let mut impacted_symbols = Vec::new();
                        for id in impacted_ids {
                            if let Some(sym) = indexer.get_symbol(id) {
                                impacted_symbols.push(sym);
                            }
                        }

                        Some(impacted_symbols)
                    } else {
                        None // Symbol not found
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Collect data for search_symbols if JSON output is requested
            let search_symbols_data = if json && tool == "search_symbols" {
                let query = arguments
                    .as_ref()
                    .and_then(|m| m.get("query"))
                    .and_then(|v| v.as_str());

                if let Some(q) = query {
                    let limit = arguments
                        .as_ref()
                        .and_then(|m| m.get("limit"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(10) as u32;
                    let kind = arguments
                        .as_ref()
                        .and_then(|m| m.get("kind"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let module = arguments
                        .as_ref()
                        .and_then(|m| m.get("module"))
                        .and_then(|v| v.as_str());
                    let language = arguments
                        .as_ref()
                        .and_then(|m| m.get("lang"))
                        .and_then(|v| v.as_str());

                    // Parse the kind filter if provided
                    let kind_filter = kind.as_ref().and_then(|k| match k.to_lowercase().as_str() {
                        "function" => Some(crate::SymbolKind::Function),
                        "struct" => Some(crate::SymbolKind::Struct),
                        "trait" => Some(crate::SymbolKind::Trait),
                        "method" => Some(crate::SymbolKind::Method),
                        "field" => Some(crate::SymbolKind::Field),
                        "module" => Some(crate::SymbolKind::Module),
                        "constant" => Some(crate::SymbolKind::Constant),
                        _ => None,
                    });

                    match indexer.search(q, limit as usize, kind_filter, module, language) {
                        Ok(results) => Some(results),
                        Err(_) => Some(Vec::new()),
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Collect data for semantic_search_docs if JSON output is requested
            #[derive(serde::Serialize)]
            struct SemanticSearchResult {
                symbol: Symbol,
                score: f32,
            }

            #[derive(serde::Serialize)]
            struct SemanticSearchWithContextResult {
                symbol: Symbol,
                score: f32,
                context: codanna::symbol::context::SymbolContext,
            }

            // Get guidance config before moving indexer
            let guidance_config = indexer.settings().guidance.clone();

            let semantic_search_docs_data = if json && tool == "semantic_search_docs" {
                if !indexer.has_semantic_search() {
                    None // Semantic search not enabled
                } else {
                    let query = arguments
                        .as_ref()
                        .and_then(|m| m.get("query"))
                        .and_then(|v| v.as_str());

                    if let Some(q) = query {
                        let limit = arguments
                            .as_ref()
                            .and_then(|m| m.get("limit"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(10) as usize;
                        let threshold = arguments
                            .as_ref()
                            .and_then(|m| m.get("threshold"))
                            .and_then(|v| v.as_f64())
                            .map(|t| t as f32);
                        let language = arguments
                            .as_ref()
                            .and_then(|m| m.get("lang"))
                            .and_then(|v| v.as_str());

                        let results = match threshold {
                            Some(t) => indexer.semantic_search_docs_with_threshold_and_language(
                                q, limit, t, language,
                            ),
                            None => indexer.semantic_search_docs_with_language(q, limit, language),
                        };

                        match results {
                            Ok(results) => {
                                let semantic_results: Vec<SemanticSearchResult> = results
                                    .into_iter()
                                    .map(|(symbol, score)| SemanticSearchResult { symbol, score })
                                    .collect();
                                Some(semantic_results)
                            }
                            Err(_) => Some(Vec::new()),
                        }
                    } else {
                        None
                    }
                }
            } else {
                None
            };

            // Collect data for semantic_search_with_context if JSON output is requested
            let semantic_search_with_context_data = if json
                && tool == "semantic_search_with_context"
            {
                if !indexer.has_semantic_search() {
                    None // Semantic search not enabled
                } else {
                    let query = arguments
                        .as_ref()
                        .and_then(|m| m.get("query"))
                        .and_then(|v| v.as_str());

                    if let Some(q) = query {
                        let limit = arguments
                            .as_ref()
                            .and_then(|m| m.get("limit"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(5) as u32; // Default 5 for context version
                        let threshold = arguments
                            .as_ref()
                            .and_then(|m| m.get("threshold"))
                            .and_then(|v| v.as_f64())
                            .map(|t| t as f32);
                        let language = arguments
                            .as_ref()
                            .and_then(|m| m.get("lang"))
                            .and_then(|v| v.as_str());

                        let search_results = match threshold {
                            Some(t) => indexer.semantic_search_docs_with_threshold_and_language(
                                q,
                                limit as usize,
                                t,
                                language,
                            ),
                            None => indexer.semantic_search_docs_with_language(
                                q,
                                limit as usize,
                                language,
                            ),
                        };

                        match search_results {
                            Ok(results) => {
                                use codanna::symbol::context::ContextIncludes;
                                let context_results: Vec<SemanticSearchWithContextResult> = results
                                    .into_iter()
                                    .filter_map(|(symbol, score)| {
                                        // Get full context for each symbol
                                        let context = indexer.get_symbol_context(
                                            symbol.id,
                                            ContextIncludes::CALLERS
                                                | ContextIncludes::CALLS
                                                | ContextIncludes::IMPLEMENTATIONS
                                                | ContextIncludes::DEFINITIONS,
                                        );

                                        context.map(|ctx| SemanticSearchWithContextResult {
                                            symbol,
                                            score,
                                            context: ctx,
                                        })
                                    })
                                    .collect();
                                Some(context_results)
                            }
                            Err(_) => Some(Vec::new()),
                        }
                    } else {
                        None
                    }
                }
            } else {
                None
            };

            // Check semantic search status before moving indexer
            let has_semantic_search = indexer.has_semantic_search();

            // If we need JSON output for get_index_info, collect data before moving indexer
            let index_info_data = if json && tool == "get_index_info" {
                let symbol_count = indexer.symbol_count();
                let file_count = indexer.file_count();
                let relationship_count = indexer.relationship_count();

                // Count symbols by kind
                let mut kind_counts = std::collections::HashMap::new();
                for symbol in indexer.get_all_symbols() {
                    *kind_counts.entry(symbol.kind).or_insert(0) += 1;
                }

                let functions = *kind_counts.get(&crate::SymbolKind::Function).unwrap_or(&0);
                let methods = *kind_counts.get(&crate::SymbolKind::Method).unwrap_or(&0);
                let structs = *kind_counts.get(&crate::SymbolKind::Struct).unwrap_or(&0);
                let traits = *kind_counts.get(&crate::SymbolKind::Trait).unwrap_or(&0);

                // Get semantic search info
                let semantic_search = if let Some(metadata) = indexer.get_semantic_metadata() {
                    SemanticSearchInfo {
                        enabled: true,
                        model_name: Some(metadata.model_name),
                        embeddings: Some(metadata.embedding_count),
                        dimensions: Some(metadata.dimension),
                        created: Some(codanna::mcp::format_relative_time(metadata.created_at)),
                        updated: Some(codanna::mcp::format_relative_time(metadata.updated_at)),
                    }
                } else {
                    SemanticSearchInfo {
                        enabled: false,
                        model_name: None,
                        embeddings: None,
                        dimensions: None,
                        created: None,
                        updated: None,
                    }
                };

                Some(IndexInfo {
                    symbol_count,
                    file_count: file_count as usize,
                    relationship_count,
                    symbol_kinds: SymbolKindBreakdown {
                        functions,
                        methods,
                        structs,
                        traits,
                    },
                    semantic_search,
                })
            } else {
                None
            };

            // Embedded mode - use already loaded indexer directly
            let server = codanna::mcp::CodeIntelligenceServer::new(indexer);

            // Call the tool directly
            use codanna::mcp::*;
            use rmcp::handler::server::wrapper::Parameters;

            let result = match tool.as_str() {
                "find_symbol" => {
                    let name = arguments
                        .as_ref()
                        .and_then(|m| m.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: find_symbol requires 'name' parameter");
                            std::process::exit(1);
                        });
                    let lang = arguments
                        .as_ref()
                        .and_then(|m| m.get("lang"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    server
                        .find_symbol(Parameters(FindSymbolRequest {
                            name: name.to_string(),
                            lang,
                        }))
                        .await
                }
                "get_calls" => {
                    let function_name = arguments
                        .as_ref()
                        .and_then(|m| m.get("function_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: get_calls requires 'function_name' parameter");
                            std::process::exit(1);
                        });
                    server
                        .get_calls(Parameters(GetCallsRequest {
                            function_name: function_name.to_string(),
                        }))
                        .await
                }
                "find_callers" => {
                    let function_name = arguments
                        .as_ref()
                        .and_then(|m| m.get("function_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: find_callers requires 'function_name' parameter");
                            std::process::exit(1);
                        });
                    server
                        .find_callers(Parameters(FindCallersRequest {
                            function_name: function_name.to_string(),
                        }))
                        .await
                }
                "analyze_impact" => {
                    let symbol_name = arguments
                        .as_ref()
                        .and_then(|m| m.get("symbol_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: analyze_impact requires 'symbol_name' parameter");
                            std::process::exit(1);
                        });
                    let max_depth = arguments
                        .as_ref()
                        .and_then(|m| m.get("max_depth"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(3) as u32;
                    server
                        .analyze_impact(Parameters(AnalyzeImpactRequest {
                            symbol_name: symbol_name.to_string(),
                            max_depth,
                        }))
                        .await
                }
                "get_index_info" => server.get_index_info().await,
                "search_symbols" => {
                    let query = arguments
                        .as_ref()
                        .and_then(|m| m.get("query"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: search_symbols requires 'query' parameter");
                            std::process::exit(1);
                        });
                    let limit = arguments
                        .as_ref()
                        .and_then(|m| m.get("limit"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(10) as u32;
                    let kind = arguments
                        .as_ref()
                        .and_then(|m| m.get("kind"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let module = arguments
                        .as_ref()
                        .and_then(|m| m.get("module"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let lang = arguments
                        .as_ref()
                        .and_then(|m| m.get("lang"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    server
                        .search_symbols(Parameters(SearchSymbolsRequest {
                            query: query.to_string(),
                            limit,
                            kind,
                            module,
                            lang,
                        }))
                        .await
                }
                "semantic_search_docs" => {
                    let query = arguments
                        .as_ref()
                        .and_then(|m| m.get("query"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!("Error: semantic_search_docs requires 'query' parameter");
                            std::process::exit(1);
                        });
                    let limit = arguments
                        .as_ref()
                        .and_then(|m| m.get("limit"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(10) as u32;
                    let threshold = arguments
                        .as_ref()
                        .and_then(|m| m.get("threshold"))
                        .and_then(|v| v.as_f64())
                        .map(|v| v as f32);
                    let lang = arguments
                        .as_ref()
                        .and_then(|m| m.get("lang"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    server
                        .semantic_search_docs(Parameters(SemanticSearchRequest {
                            query: query.to_string(),
                            limit,
                            threshold,
                            lang,
                        }))
                        .await
                }
                "semantic_search_with_context" => {
                    let query = arguments
                        .as_ref()
                        .and_then(|m| m.get("query"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_else(|| {
                            eprintln!(
                                "Error: semantic_search_with_context requires 'query' parameter"
                            );
                            std::process::exit(1);
                        });
                    let limit = arguments
                        .as_ref()
                        .and_then(|m| m.get("limit"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(5) as u32;
                    let threshold = arguments
                        .as_ref()
                        .and_then(|m| m.get("threshold"))
                        .and_then(|v| v.as_f64())
                        .map(|v| v as f32);
                    let lang = arguments
                        .as_ref()
                        .and_then(|m| m.get("lang"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    server
                        .semantic_search_with_context(Parameters(
                            SemanticSearchWithContextRequest {
                                query: query.to_string(),
                                limit,
                                threshold,
                                lang,
                            },
                        ))
                        .await
                }
                _ => {
                    if json {
                        use codanna::io::exit_code::ExitCode;
                        use codanna::io::format::JsonResponse;
                        let response = JsonResponse::error(
                            ExitCode::GeneralError,
                            &format!("Unknown tool: {tool}"),
                            vec![
                                "Available tools: find_symbol, get_calls, find_callers, analyze_impact, get_index_info, search_symbols, semantic_search_docs, semantic_search_with_context",
                            ],
                        );
                        println!("{}", serde_json::to_string_pretty(&response).unwrap());
                    } else {
                        eprintln!("Unknown tool: {tool}");
                        eprintln!(
                            "Available tools: find_symbol, get_calls, find_callers, analyze_impact, get_index_info, search_symbols, semantic_search_docs, semantic_search_with_context"
                        );
                    }
                    std::process::exit(1);
                }
            };

            // Print result
            match result {
                Ok(call_result) => {
                    if json && tool == "get_index_info" {
                        // Use pre-collected data for JSON output
                        if let Some(index_info) = index_info_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            let mut response = JsonResponse::success(index_info);

                            // Add system guidance (using single_result template since this returns stats)
                            if let Some(guidance) = generate_guidance_from_config(
                                &guidance_config,
                                "get_index_info",
                                None,
                                1,
                            ) {
                                // Use 1 to trigger single_result template
                                response = response.with_system_message(&guidance);
                            }

                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                        }
                    } else if json && tool == "find_symbol" {
                        // Use pre-collected data for JSON output
                        if let Some(symbol_contexts) = find_symbol_data {
                            use codanna::io::format::JsonResponse;
                            if symbol_contexts.is_empty() {
                                let name = arguments
                                    .as_ref()
                                    .and_then(|m| m.get("name"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");
                                let response = JsonResponse::not_found("Symbol", name);
                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                                std::process::exit(3);
                            } else {
                                use codanna::io::guidance_engine::generate_guidance_from_config;
                                let mut response = JsonResponse::success(symbol_contexts);

                                // Add system guidance
                                let result_count =
                                    response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "find_symbol",
                                    arguments
                                        .as_ref()
                                        .and_then(|m| m.get("name"))
                                        .and_then(|v| v.as_str()),
                                    result_count,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            }
                        }
                    } else if json && tool == "get_calls" {
                        // Use pre-collected data for JSON output
                        if let Some(calls) = get_calls_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            let mut response = JsonResponse::success(calls);

                            // Add system guidance
                            let result_count = response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                            if let Some(guidance) = generate_guidance_from_config(
                                &guidance_config,
                                "get_calls",
                                arguments
                                    .as_ref()
                                    .and_then(|m| m.get("function_name"))
                                    .and_then(|v| v.as_str()),
                                result_count,
                            ) {
                                response = response.with_system_message(&guidance);
                            }

                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                        } else {
                            // Function not found
                            let name = arguments
                                .as_ref()
                                .and_then(|m| m.get("function_name"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::not_found("Function", name);
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(3);
                        }
                    } else if json && tool == "find_callers" {
                        // Use pre-collected data for JSON output
                        if let Some(callers) = find_callers_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            let mut response = JsonResponse::success(callers);

                            // Add system guidance
                            let result_count = response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                            if let Some(guidance) = generate_guidance_from_config(
                                &guidance_config,
                                "find_callers",
                                arguments
                                    .as_ref()
                                    .and_then(|m| m.get("function_name"))
                                    .and_then(|v| v.as_str()),
                                result_count,
                            ) {
                                response = response.with_system_message(&guidance);
                            }

                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                        } else {
                            // Function not found
                            let name = arguments
                                .as_ref()
                                .and_then(|m| m.get("function_name"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::not_found("Function", name);
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(3);
                        }
                    } else if json && tool == "analyze_impact" {
                        // Use pre-collected data for JSON output
                        if let Some(impacted) = analyze_impact_data {
                            use codanna::io::format::JsonResponse;
                            if impacted.is_empty() {
                                // No symbols would be impacted
                                let name = arguments
                                    .as_ref()
                                    .and_then(|m| m.get("symbol_name"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");
                                use codanna::io::guidance_engine::generate_guidance_from_config;

                                // Create a proper struct for the empty case
                                #[derive(serde::Serialize)]
                                struct EmptyImpactResult {
                                    symbol: String,
                                    impacted_count: usize,
                                    impacted_symbols: Vec<String>,
                                    message: String,
                                }

                                let impact_result = EmptyImpactResult {
                                    symbol: name.to_string(),
                                    impacted_count: 0,
                                    impacted_symbols: vec![],
                                    message:
                                        "No symbols would be impacted by changes to this symbol"
                                            .to_string(),
                                };

                                let mut response = JsonResponse::success(impact_result);

                                // Add guidance for no results case
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "analyze_impact",
                                    Some(name),
                                    0,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            } else {
                                use codanna::io::guidance_engine::generate_guidance_from_config;
                                let mut response = JsonResponse::success(impacted);

                                // Add system guidance
                                let result_count =
                                    response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "analyze_impact",
                                    arguments
                                        .as_ref()
                                        .and_then(|m| m.get("symbol_name"))
                                        .and_then(|v| v.as_str()),
                                    result_count,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            }
                        } else {
                            // Symbol not found
                            let name = arguments
                                .as_ref()
                                .and_then(|m| m.get("symbol_name"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::not_found("Symbol", name);
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(3);
                        }
                    } else if json && tool == "search_symbols" {
                        // Use pre-collected data for JSON output
                        if let Some(results) = search_symbols_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            if results.is_empty() {
                                // Create proper struct for empty search results
                                #[derive(serde::Serialize)]
                                struct EmptySearchResult {
                                    query: String,
                                    result_count: usize,
                                    results: Vec<String>,
                                    message: String,
                                }

                                let query = arguments
                                    .as_ref()
                                    .and_then(|m| m.get("query"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");

                                let search_result = EmptySearchResult {
                                    query: query.to_string(),
                                    result_count: 0,
                                    results: vec![],
                                    message: "No results found for query".to_string(),
                                };

                                let mut response = JsonResponse::success(search_result);

                                // Add guidance for no results
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "search_symbols",
                                    Some(query),
                                    0,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            } else {
                                use codanna::io::guidance_engine::generate_guidance_from_config;
                                let mut response = JsonResponse::success(results);

                                // Add system guidance
                                let result_count =
                                    response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "search_symbols",
                                    arguments
                                        .as_ref()
                                        .and_then(|m| m.get("query"))
                                        .and_then(|v| v.as_str()),
                                    result_count,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            }
                        } else {
                            use codanna::io::exit_code::ExitCode;
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::error(
                                ExitCode::GeneralError,
                                "Failed to execute search",
                                vec!["Check query syntax"],
                            );
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(1);
                        }
                    } else if json && tool == "semantic_search_docs" {
                        // Use pre-collected data for JSON output
                        if let Some(results) = semantic_search_docs_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            if results.is_empty() {
                                // Create proper struct for empty semantic search
                                #[derive(serde::Serialize)]
                                struct EmptySemanticResult {
                                    query: String,
                                    result_count: usize,
                                    results: Vec<String>,
                                    message: String,
                                }

                                let query = arguments
                                    .as_ref()
                                    .and_then(|m| m.get("query"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");

                                let semantic_result = EmptySemanticResult {
                                    query: query.to_string(),
                                    result_count: 0,
                                    results: vec![],
                                    message: "No semantically similar documentation found"
                                        .to_string(),
                                };

                                let mut response = JsonResponse::success(semantic_result);

                                // Add guidance for no results
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "semantic_search_docs",
                                    Some(query),
                                    0,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            } else {
                                let mut response = JsonResponse::success(results);

                                // Add system guidance for AI assistants
                                let result_count =
                                    response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "semantic_search_docs",
                                    arguments
                                        .as_ref()
                                        .and_then(|m| m.get("query"))
                                        .and_then(|v| v.as_str()),
                                    result_count,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            }
                        } else if !has_semantic_search {
                            use codanna::io::exit_code::ExitCode;
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::error(
                                ExitCode::GeneralError,
                                "Semantic search is not enabled",
                                vec![
                                    "Enable semantic search in settings.toml and rebuild the index",
                                ],
                            );
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(1);
                        } else {
                            use codanna::io::exit_code::ExitCode;
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::error(
                                ExitCode::GeneralError,
                                "Failed to execute semantic search",
                                vec!["Check query syntax"],
                            );
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(1);
                        }
                    } else if json && tool == "semantic_search_with_context" {
                        // Use pre-collected data for JSON output
                        if let Some(results) = semantic_search_with_context_data {
                            use codanna::io::format::JsonResponse;
                            use codanna::io::guidance_engine::generate_guidance_from_config;
                            if results.is_empty() {
                                // Create proper struct for empty semantic search with context
                                #[derive(serde::Serialize)]
                                struct EmptyContextResult {
                                    query: String,
                                    result_count: usize,
                                    results: Vec<String>,
                                    message: String,
                                }

                                let query = arguments
                                    .as_ref()
                                    .and_then(|m| m.get("query"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");

                                let context_result = EmptyContextResult {
                                    query: query.to_string(),
                                    result_count: 0,
                                    results: vec![],
                                    message: "No semantically similar documentation found"
                                        .to_string(),
                                };

                                let mut response = JsonResponse::success(context_result);

                                // Add guidance for no results
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "semantic_search_with_context",
                                    Some(query),
                                    0,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            } else {
                                use codanna::io::guidance_engine::generate_guidance_from_config;
                                let mut response = JsonResponse::success(results);

                                // Add system guidance
                                let result_count =
                                    response.data.as_ref().map(|d| d.len()).unwrap_or(0);
                                if let Some(guidance) = generate_guidance_from_config(
                                    &guidance_config,
                                    "semantic_search_with_context",
                                    arguments
                                        .as_ref()
                                        .and_then(|m| m.get("query"))
                                        .and_then(|v| v.as_str()),
                                    result_count,
                                ) {
                                    response = response.with_system_message(&guidance);
                                }

                                println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            }
                        } else if !has_semantic_search {
                            use codanna::io::exit_code::ExitCode;
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::error(
                                ExitCode::GeneralError,
                                "Semantic search is not enabled",
                                vec![
                                    "Enable semantic search in settings.toml and rebuild the index",
                                ],
                            );
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(1);
                        } else {
                            use codanna::io::exit_code::ExitCode;
                            use codanna::io::format::JsonResponse;
                            let response = JsonResponse::error(
                                ExitCode::GeneralError,
                                "Failed to execute semantic search with context",
                                vec!["Check query syntax"],
                            );
                            println!("{}", serde_json::to_string_pretty(&response).unwrap());
                            std::process::exit(1);
                        }
                    } else {
                        // Default text output
                        for content in &call_result.content {
                            match &**content {
                                rmcp::model::RawContent::Text(text_content) => {
                                    println!("{}", text_content.text);
                                }
                                _ => {
                                    eprintln!("Warning: Non-text content returned");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if json {
                        use codanna::io::exit_code::ExitCode;
                        use codanna::io::format::JsonResponse;
                        let response = JsonResponse::error(
                            ExitCode::GeneralError,
                            &e.message,
                            vec!["Check the tool name and arguments"],
                        );
                        println!("{}", serde_json::to_string_pretty(&response).unwrap());
                        std::process::exit(1);
                    } else {
                        eprintln!("Error calling tool: {}", e.message);
                        std::process::exit(1);
                    }
                }
            }
        }

        Commands::Benchmark { language, file } => {
            run_benchmark_command(&language, file);
        }
    }
}

/// Run parser performance benchmarks
fn run_benchmark_command(language: &str, custom_file: Option<PathBuf>) {
    use codanna::display::theme::Theme;
    use console::style;

    // Print styled header
    if Theme::should_disable_colors() {
        println!("\n=== Codanna Parser Benchmarks ===\n");
    } else {
        println!(
            "\n{}\n",
            style("=== Codanna Parser Benchmarks ===").cyan().bold()
        );
    }

    match language.to_lowercase().as_str() {
        "rust" => benchmark_rust_parser(custom_file),
        "python" => benchmark_python_parser(custom_file),
        "php" => benchmark_php_parser(custom_file),
        "typescript" | "ts" => benchmark_typescript_parser(custom_file),
        "go" => benchmark_go_parser(custom_file),
        "all" => {
            benchmark_rust_parser(None);
            println!();
            benchmark_python_parser(None);
            println!();
            benchmark_php_parser(None);
            println!();
            benchmark_typescript_parser(None);
            println!();
            benchmark_go_parser(None);
        }
        _ => {
            eprintln!("Unknown language: {language}");
            eprintln!("Available languages: rust, python, php, typescript, go, all");
            std::process::exit(1);
        }
    }

    // Print target info with styling
    if Theme::should_disable_colors() {
        println!("\nTarget: >10,000 symbols/second ✅");
    } else {
        println!(
            "\n{}: {} ✅",
            style("Target").dim(),
            style(">10,000 symbols/second").dim()
        );
    }
}

fn benchmark_rust_parser(custom_file: Option<PathBuf>) {
    let (code, file_path) = if let Some(path) = custom_file {
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("Failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        (content, Some(path))
    } else {
        // Generate benchmark code
        (generate_rust_benchmark_code(), None)
    };

    let mut parser = RustParser::new().expect("Failed to create Rust parser");
    benchmark_parser("Rust", &mut parser, &code, file_path);
}

fn benchmark_python_parser(custom_file: Option<PathBuf>) {
    let (code, file_path) = if let Some(path) = custom_file {
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("Failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        (content, Some(path))
    } else if std::path::Path::new("tests/python_comprehensive_features.py").exists() {
        match std::fs::read_to_string("tests/python_comprehensive_features.py") {
            Ok(content) => (content, None),
            Err(e) => {
                eprintln!("Warning: Failed to read test file: {e}");
                eprintln!("Generating benchmark code instead...");
                (generate_python_benchmark_code(), None)
            }
        }
    } else {
        // Generate benchmark code
        (generate_python_benchmark_code(), None)
    };

    let mut parser = PythonParser::new().expect("Failed to create Python parser");
    benchmark_parser("Python", &mut parser, &code, file_path);
}

fn benchmark_php_parser(custom_file: Option<PathBuf>) {
    let (code, file_path) = if let Some(path) = custom_file {
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("Failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        (content, Some(path))
    } else {
        // Generate benchmark code
        (generate_php_benchmark_code(), None)
    };

    let mut parser = PhpParser::new().expect("Failed to create PHP parser");
    benchmark_parser("PHP", &mut parser, &code, file_path);
}

fn benchmark_typescript_parser(custom_file: Option<PathBuf>) {
    let (code, file_path) = if let Some(path) = custom_file {
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("Failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        (content, Some(path))
    } else {
        // Generate benchmark code
        (generate_typescript_benchmark_code(), None)
    };

    let mut parser = TypeScriptParser::new().expect("Failed to create TypeScript parser");
    benchmark_parser("TypeScript", &mut parser, &code, file_path);
}

fn benchmark_go_parser(custom_file: Option<PathBuf>) {
    let (code, file_path) = if let Some(path) = custom_file {
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("Failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        (content, Some(path))
    } else {
        // Generate benchmark code
        (generate_go_benchmark_code(), None)
    };

    let mut parser = GoParser::new().expect("Failed to create Go parser");
    benchmark_parser("Go", &mut parser, &code, file_path);
}

fn benchmark_parser(
    language: &str,
    parser: &mut dyn LanguageParser,
    code: &str,
    file_path: Option<PathBuf>,
) {
    let file_id = FileId::new(1).expect("Failed to create file ID");
    let mut counter = SymbolCounter::new();

    // Warm up
    let _ = parser.parse(code, file_id, &mut counter);

    // Measure parsing performance (average of 3 runs)
    let mut total_duration = std::time::Duration::ZERO;
    let mut symbols_count = 0;

    for _ in 0..3 {
        counter = SymbolCounter::new();
        let start = Instant::now();
        let symbols = parser.parse(code, file_id, &mut counter);
        total_duration += start.elapsed();
        symbols_count = symbols.len();
    }

    let avg_duration = total_duration / 3;
    let rate = symbols_count as f64 / avg_duration.as_secs_f64();

    // Display results using rich table
    use codanna::display::tables::create_benchmark_table;

    let table = create_benchmark_table(
        language,
        file_path
            .as_ref()
            .map(|p| p.to_str().unwrap_or("<invalid path>")),
        symbols_count,
        avg_duration,
        rate,
    );

    println!("\n{table}");

    // Verify zero-cost abstractions (silently)
    let calls = parser.find_calls(code);
    if !calls.is_empty() {
        let (caller, _callee, _) = &calls[0];
        let caller_ptr = caller.as_ptr();
        let code_ptr = code.as_ptr();
        let within_bounds =
            caller_ptr >= code_ptr && caller_ptr < unsafe { code_ptr.add(code.len()) };

        if !within_bounds {
            println!("\n⚠️  Warning: String allocation detected!");
        }
    }
}

fn generate_rust_benchmark_code() -> String {
    let mut code = String::from("//! Rust benchmark file\n\n");

    // Generate 500 functions
    for i in 0..500 {
        code.push_str(&format!(
            r#"/// Function {i} documentation
fn function_{i}(param1: i32, param2: &str) -> bool {{
    let result = param1 * 2;
    result > 0
}}

"#
        ));
    }

    // Generate 50 structs with methods
    for i in 0..50 {
        code.push_str(&format!(
            r#"/// Struct {i} documentation
struct Struct{i} {{
    value: i32,
}}

impl Struct{i} {{
    fn new(value: i32) -> Self {{
        Self {{ value }}
    }}

    fn method_a(&self) -> i32 {{
        self.value * 2
    }}
}}

"#
        ));
    }

    code
}

fn generate_python_benchmark_code() -> String {
    let mut code = String::from("\"\"\"Python benchmark file\"\"\"\n\n");

    // Generate 500 functions
    for i in 0..500 {
        code.push_str(&format!(
            r#"def function_{i}(param1: int, param2: str = 'default') -> bool:
    """Function {i} documentation."""
    result = param1 * 2
    return result > 0

"#
        ));
    }

    // Generate 50 classes
    for i in 0..50 {
        code.push_str(&format!(
            r#"class Class_{i}:
    """Class {i} documentation."""

    def __init__(self, value: int):
        self.value = value

    def method_a(self) -> int:
        return self.value * 2

"#
        ));
    }

    code
}

fn generate_php_benchmark_code() -> String {
    let mut code = String::from("<?php\n/**\n * PHP benchmark file\n */\n\n");

    // Generate 500 functions
    for i in 0..500 {
        code.push_str(&format!(
            r#"/**
 * Function {i} documentation
 */
function function_{i}(int $param1, string $param2 = 'default'): bool {{
    $result = $param1 * 2;
    return $result > 0;
}}

"#
        ));
    }

    // Generate 50 classes with methods
    for i in 0..50 {
        code.push_str(&format!(
            r#"/**
 * Class {i} documentation
 */
class Class_{i} {{
    private int $value;

    public function __construct(int $value) {{
        $this->value = $value;
    }}

    public function methodA(): int {{
        return $this->value * 2;
    }}

    public function methodB(string $param): string {{
        return strtoupper($param);
    }}
}}

"#
        ));
    }

    // Generate 25 interfaces
    for i in 0..25 {
        code.push_str(&format!(
            r#"interface Interface_{i} {{
    public function method_{i}(): void;
}}

"#
        ));
    }

    // Generate 25 traits
    for i in 0..25 {
        code.push_str(&format!(
            r#"trait Trait_{i} {{
    public function traitMethod_{i}(): string {{
        return 'trait_{i}';
    }}
}}

"#
        ));
    }

    code.push_str("?>");
    code
}

fn generate_typescript_benchmark_code() -> String {
    let mut code = String::from("// TypeScript benchmark file\n\n");

    // Generate 500 functions with various TypeScript features
    for i in 0..500 {
        code.push_str(&format!(
            r#"/**
 * Function {i} documentation
 * @param param1 The first parameter
 * @param param2 The second parameter
 * @returns A boolean result
 */
export function function_{i}(param1: number, param2: string = 'default'): boolean {{
    const result = param1 > 0 && param2.length > 0;
    return result;
}}

"#
        ));
    }

    // Generate 50 interfaces
    for i in 0..50 {
        code.push_str(&format!(
            r#"/**
 * Interface {i} for data structure
 */
export interface Interface_{i} {{
    id: number;
    name: string;
    optional?: boolean;
    readonly immutable: string;
    method(param: string): void;
}}

"#
        ));
    }

    // Generate 50 classes with methods
    for i in 0..50 {
        code.push_str(&format!(
            r#"/**
 * Class {i} implementation
 */
export class Class_{i} implements Interface_{i} {{
    public id: number;
    public name: string;
    public optional?: boolean;
    public readonly immutable: string;
    private _internal: number;
    protected _protected: string;

    constructor(id: number, name: string) {{
        this.id = id;
        this.name = name;
        this.immutable = 'fixed';
        this._internal = 0;
        this._protected = 'protected';
    }}

    public method(param: string): void {{
        console.log(param);
    }}

    private privateMethod(): number {{
        return this._internal;
    }}

    protected protectedMethod(): string {{
        return this._protected;
    }}

    static staticMethod(): void {{
        console.log('static');
    }}
}}

"#
        ));
    }

    // Generate 50 type aliases
    for i in 0..50 {
        code.push_str(&format!(
            r#"/**
 * Type alias {i}
 */
export type TypeAlias_{i} = string | number | boolean;

type ComplexType_{i} = {{
    field1: TypeAlias_{i};
    field2: Interface_{i};
    field3: (param: string) => void;
}};

"#
        ));
    }

    // Generate 50 enums
    for i in 0..50 {
        code.push_str(&format!(
            r#"/**
 * Enum {i} definition
 */
export enum Enum_{i} {{
    First = 0,
    Second = 1,
    Third = 'three',
    Fourth = 'four'
}}

"#
        ));
    }

    // Add some arrow functions and const declarations
    for i in 0..50 {
        code.push_str(&format!(
            r#"export const arrowFunction_{i} = (x: number, y: number): number => x + y;

export const constant_{i}: string = 'constant value';

let variable_{i}: number = {i};

"#
        ));
    }

    code
}

fn generate_go_benchmark_code() -> String {
    let mut code =
        String::from("// Go benchmark file\n\npackage bench\n\nimport (\n\t\"fmt\"\n)\n\n");

    // Generate 500 free functions
    for i in 0..500 {
        code.push_str(&format!(
            r#"// Function {i} documentation
func Function_{i}(param1 int, param2 string) bool {{
    result := param1 * 2
    return result > 0 && len(param2) > 0
}}

"#
        ));
    }

    // Generate 50 structs with methods and interface satisfaction
    for i in 0..50 {
        code.push_str(&format!(
            r#"// Struct {i} documentation
type Struct{i} struct {{
    Value int
}}

func NewStruct{i}(v int) *Struct{i} {{
    return &Struct{i}{{Value: v}}
}}

func (s *Struct{i}) MethodA() int {{
    return s.Value * 2
}}

func (s *Struct{i}) Do(param string) int {{
    fmt.Println(param)
    return len(param) + s.Value
}}

"#
        ));
    }

    // Generate 25 interfaces
    for i in 0..25 {
        code.push_str(&format!(
            r#"// Interface {i} documentation
type Interface_{i} interface {{
    Do(param string) int
}}

"#
        ));
    }

    // A small main-like entry to keep parser busy with calls/selectors
    code.push_str(
        r#"// Entry point (not used, just for call patterns)
func main() {
    s := NewStruct0(42)
    _ = s.MethodA()
    _ = s.Do("hello")
    ok := Function_0(1, "x")
    if ok {
        fmt.Println("ok")
    }
}
"#,
    );

    code
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// Verifies CLI structure is valid at compile time.
    ///
    /// Uses clap's debug_assert to catch configuration errors.
    #[test]
    fn verify_cli() {
        // This test ensures the CLI structure is valid
        Cli::command().debug_assert();
    }
}
