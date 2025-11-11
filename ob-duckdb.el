;;; ob-duckdb.el --- Org Babel functions for DuckDB SQL -*- lexical-binding: t; -*-

;; Author: gggion
;; Maintainer: gggion <gggion123@gmail.com>
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.4"))
;; Keywords: languages, org, babel, duckdb, sql, data, analytics
;; URL: https://github.com/gggion/ob-duckdb

;; This file is NOT part of GNU Emacs.

;;; Commentary:

;; This package provides Org Babel integration for DuckDB, an in-process
;; analytical SQL database engine.
;;
;; DuckDB (https://duckdb.org/) combines analytical performance with a
;; familiar SQL interface. ob-duckdb brings these capabilities directly into
;; Org documents for data exploration and literate analytics.
;;
;; Basic usage:
;;
;;     #+begin_src duckdb
;;     SELECT * FROM read_csv('data.csv') LIMIT 10;
;;     #+end_src
;;
;; With persistent database:
;;
;;     #+begin_src duckdb :db /path/to/database.duckdb
;;     CREATE TABLE analysis AS SELECT * FROM read_parquet('large.parquet');
;;     SELECT COUNT(*) FROM analysis;
;;     #+end_src
;;
;; Asynchronous execution (requires session):
;;
;;     #+begin_src duckdb :session work :async yes
;;     SELECT * FROM huge_table WHERE complex_condition;
;;     #+end_src
;;
;; The package supports:
;; - Session-based execution for stateful workflows
;; - Multiple output formats (box, csv, json, markdown, etc.)
;; - Variable substitution from Org elements
;; - DuckDB dot commands for configuration
;; - Automatic result truncation for large outputs
;; - Progress monitoring for long-running queries
;; - FIFO queue for multiple async executions per session
;;
;; Block tracking is optional and disabled by default:
;;
;;     ;; Minimal mode (default) - only async execution tracking
;;     (require 'ob-duckdb)
;;
;;     ;; Full tracking mode - execution history and debugging
;;     (require 'org-duckdb-blocks)
;;     (setq org-duckdb-blocks-enable-tracking t
;;           org-duckdb-blocks-visible-properties nil) ; invisible by default
;;     (org-duckdb-blocks-setup)
;;
;; With tracking disabled, async executions still work correctly via
;; exec-id embedded in result placeholders. Full tracking adds:
;; - Execution history (`org-duckdb-blocks-recent')
;; - Block registry (`org-duckdb-blocks-list')
;; - Navigation commands (`org-duckdb-blocks-navigate-recent')
;; - Enhanced cancellation support
;;
;; Property storage modes (only when tracking enabled):
;; - Invisible (default): Text properties, no document pollution
;; - Visible: #+PROPERTY: lines for inspection
;;
;; Key commands:
;; - `org-babel-duckdb-create-session' - Start a session
;; - `org-babel-duckdb-display-sessions' - Show active sessions
;; - `org-babel-duckdb-show-queue' - Monitor async execution queue
;; - `org-babel-duckdb-cancel-execution' - Cancel async execution in queue

;;; Code:

(require 'ob)
(require 'org-element)
(require 'ansi-color)

(defvar org-babel-default-header-args:duckdb
  '((:results . "output")
    (:wrap))
  "Default header arguments for DuckDB source blocks.
Uses output mode with wrapping enabled to preserve DuckDB's formatted
output including box-drawing characters and ANSI colors.

Also see `org-babel-header-args:duckdb' for DuckDB-specific arguments.")

(defconst org-babel-header-args:duckdb
  '((db        . :any)
    (format    . :any)
    (timer     . :any)
    (headers   . :any)
    (nullvalue . :any)
    (separator . :any)
    (echo      . :any)
    (bail      . :any)
    (session   . :any)
    (async     . :any)
    (output    . :any)
    (max-rows  . :any))
  "DuckDB-specific header arguments for source blocks.

For standard Org Babel arguments, see Info node `(org) Using Header Arguments'.")

;;;; Customization Group and User Options
(defgroup org-babel-duckdb nil
  "Org Babel support for DuckDB analytical queries."
  :group 'org-babel
  :prefix "org-babel-duckdb-"
  :link '(url-link :tag "Homepage" "https://github.com/gggion/ob-duckdb")
  :link '(emacs-commentary-link :tag "Commentary" "ob-duckdb"))

;;;;; Execution Control Options

(defcustom org-babel-duckdb-command "duckdb"
  "Command to invoke the DuckDB CLI executable.
Can be \"duckdb\" if in PATH, or an absolute/relative path.

Used by `org-babel-duckdb-execute-sync' for direct invocation and
`org-babel-duckdb-initiate-session' for session processes."
  :type 'string
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

;;;;; Output and Display Options

(defcustom org-babel-duckdb-output-buffer "*DuckDB-output*"
  "Buffer name for query results when :output buffer is used.

See `org-babel-duckdb-display-buffer' for display behavior and
`org-babel-duckdb--insert-result-at-point' for when this is used."
  :type 'string
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

(defcustom org-babel-duckdb-prompt-char "⬤◗"
  "Character used as DuckDB prompt in session buffers.
Must be unique enough not to appear in query results, as all occurrences
are removed from output by `org-babel-duckdb-clean-output'.

Examples of good prompt characters:
  \"🦆\"
  \"⬤➤\"
  \"⬤◗\"  (default)
  \"D>\"

Set by `org-babel-duckdb-initiate-session' when creating sessions."
  :type 'string
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

(defcustom org-babel-duckdb-max-rows 200000
  "Maximum number of output lines to display from query results.

Prevents Emacs from freezing when inserting massive result sets.
Set to nil to disable truncation (may cause hangs on large outputs).

Can be overridden per-block with :max-rows header argument.

Truncation is applied by `org-babel-duckdb-read-file-lines' during
result processing. For async execution behavior, see
`org-babel-duckdb--make-completion-handler'.

Performance implications:
- Below 50k lines: Negligible overhead
- 50k-200k lines: Noticeable delay (2-5 seconds)
- Above 200k lines: Risk of UI freezing

Also see `org-babel-duckdb-output-buffer' for alternative result display."
  :type '(choice (integer :tag "Maximum rows to display")
          (const :tag "No limit (may freeze Emacs)" nil))
  :group 'org-babel-duckdb
  :link '(info-link "(org-babel-duckdb) Result Handling")
  :package-version '(ob-duckdb . "2.0.0"))

;;;;; Progress Tracking and Display Options

(defcustom org-babel-duckdb-show-progress t
  "Whether to show status indicators during asynchronous execution.

When non-nil, async queries display status via
`org-babel-duckdb-create-progress-monitor'.

Display method controlled by `org-babel-duckdb-progress-display'.

Only applies to async execution (see `org-babel-duckdb-execute-async')."
  :type 'boolean
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

(defcustom org-babel-duckdb-progress-display 'minibuffer
  "How to display progress during async execution.

Values:
  popup      - Dedicated buffer in side window
  minibuffer - One-time message in minibuffer (default)
  nil        - No progress display

When `popup, creates live-updating buffer showing execution progress.
Progress popups auto-close 3 seconds after completion.

When `minibuffer, shows initial execution message and completion message.

Independent of queue display (see `org-babel-duckdb-queue-display').

Only active when `org-babel-duckdb-show-progress' is non-nil."
  :type '(choice (const :tag "Popup buffer" popup)
          (const :tag "Minibuffer message" minibuffer)
          (const :tag "No progress display" nil))
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

(defcustom org-babel-duckdb-queue-display 'auto
  "How to display async execution queue.

Values:
  manual - Show queue only when `org-babel-duckdb-show-queue' called
  auto   - Auto-show queue when multiple executions pending (default)

When `auto, queue buffer appears automatically when second async
execution starts in same session, and closes when queue empties.

When `manual, call `org-babel-duckdb-show-queue' explicitly to monitor.

Queue position controlled by `org-babel-duckdb-queue-position'.

Also see `org-babel-duckdb-progress-display' for per-execution feedback."
  :type '(choice (const :tag "Manual display only" manual)
          (const :tag "Auto-show on queue" auto))
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

(defcustom org-babel-duckdb-queue-position 'bottom
  "Where to display queue monitor buffer.

Values:
  bottom - Bottom side window (default)
  side   - Right side window

Applies to both auto-display and manual `org-babel-duckdb-show-queue'.

Window height (bottom) or width (side) is 25% of frame.

Also see `org-babel-duckdb-queue-display' for display triggering."
  :type '(choice (const :tag "Bottom panel" bottom)
          (const :tag "Side panel" side))
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "2.0.0"))

;;;; Hooks for Extension Points

(defvar org-babel-duckdb-execution-started-functions nil
  "Abnormal hook run when DuckDB execution starts (sync or async).

Each function is called with arguments:
  (EXEC-ID SESSION BODY PARAMS IS-ASYNC-P ELEMENT)

Where:
  EXEC-ID    - UUID string for this execution
  SESSION    - Session name string or nil
  BODY       - SQL code being executed (after variable expansion)
  PARAMS     - Alist of header arguments
  IS-ASYNC-P - Non-nil if async execution
  ELEMENT    - org-element at point (source block)

Functions should not modify execution behavior, only observe.

Used by org-duckdb-blocks.el to register executions for tracking.

Also see `org-babel-duckdb-async-process-started-functions' and
`org-babel-duckdb-execution-completed-functions'.")

(defvar org-babel-duckdb-async-process-started-functions nil
  "Abnormal hook run when async process actually starts.

Each function is called with arguments:
  (EXEC-ID PROCESS)

Where:
  EXEC-ID - UUID string for this execution
  PROCESS - Process object for the running query

Called after execution is queued and process begins running.
For queued executions, this fires when execution becomes active,
not when initially queued.

Used by org-duckdb-blocks.el to store process references for cancellation.

Also see `org-babel-duckdb-execution-started-functions' and
`org-babel-duckdb-execution-completed-functions'.")

(defvar org-babel-duckdb-execution-completed-functions nil
  "Abnormal hook run after DuckDB execution completes.

Each function is called with arguments:
  (EXEC-ID STATUS ERROR-INFO)

Where:
  EXEC-ID    - UUID string for this execution
  STATUS     - Symbol: completed, error, cancelled, completed-with-errors
  ERROR-INFO - Error description string (nil if no error)

Called after results are inserted and cleanup is complete.

Used by org-duckdb-blocks.el to update execution status.

Also see `org-babel-duckdb-execution-started-functions' and
`org-babel-duckdb-async-process-started-functions'.")

;;;; Internal Variables
(defvar-local org-babel-duckdb-session-db-file nil
  "Database file path for current DuckDB session buffer.

Nil indicates an in-memory database.
Set by `org-babel-duckdb-initiate-session' during session creation.
Queried by `org-babel-duckdb-display-sessions' for display.

This is a buffer-local variable; each session buffer has its own value.")

(defvar org-babel-duckdb--pending-async (make-hash-table :test 'equal)
  "Maps exec-id to (buffer . marker) for async result routing.

Keys are execution ID strings (UUIDs).
Values are cons cells (BUFFER . MARKER) where:
  BUFFER - Buffer containing the source block
  MARKER - Marker at result insertion point

This table provides essential routing for async executions without
requiring full block tracking. Entries are removed after completion.

Populated during async execution in `org-babel-execute:duckdb'.
Queried by `org-babel-duckdb--find-result-location'.

For full execution history and debugging, enable
`org-duckdb-blocks-enable-tracking'.")

(defvar org-babel-duckdb-sessions (make-hash-table :test 'equal)
  "Hash table of active DuckDB session buffers.

Keys are session names (strings), values are buffer objects.

Managed by `org-babel-duckdb-initiate-session',
`org-babel-duckdb-delete-session', and
`org-babel-duckdb-cleanup-sessions'.

Query with `org-babel-duckdb-list-sessions'.")

(defvar org-babel-duckdb-progress-buffers (make-hash-table :test 'equal)
  "Hash table tracking progress display buffers by execution ID.

Keys are execution IDs (UUID strings), values are buffer objects.

Managed by `org-babel-duckdb-create-progress-monitor' and
`org-babel-duckdb-cleanup-progress-display'.

Only populated when `org-babel-duckdb-show-progress' is non-nil and
`org-babel-duckdb-progress-display' is `popup.")

(defvar org-babel-duckdb--session-queues (make-hash-table :test 'equal)
  "Maps session names to queue structures for FIFO async execution.

Keys are session name strings.
Values are plists with:
  :pending              - List of exec-id strings awaiting completion
  :completion-handlers  - Hash table mapping exec-id to callback functions
  :send-data            - Hash table mapping exec-id to command strings
  :temp-err-files       - Hash table mapping exec-id to error file paths
  :original-filter      - Original process filter to restore when queue empties

Enables multiple async executions in same session with strict FIFO ordering.
Only first pending execution runs at a time; completion triggers next.

Managed by `org-babel-duckdb--enqueue-execution',
`org-babel-duckdb--dequeue-execution', and
`org-babel-duckdb--session-output-filter'.")

(defvar org-babel-duckdb--queue-buffer nil
  "Buffer object for live queue monitoring display.

Nil when queue monitor is not active.
Set by `org-babel-duckdb-show-queue' when creating monitor.
Cleared when buffer is killed or all queues empty.

The buffer auto-refreshes every 0.5 seconds while executions are pending.

Also see `org-babel-duckdb--queue-refresh-timer' and
`org-babel-duckdb--stop-queue-monitor'.")

(defvar org-babel-duckdb--queue-refresh-timer nil
  "Timer for auto-refreshing queue monitor display.

Active when `org-babel-duckdb--queue-buffer' exists and queues are non-empty.
Refresh interval is 0.5 seconds for responsive updates.

Canceled by `org-babel-duckdb--stop-queue-monitor' when all queues complete.

Also see `org-babel-duckdb--refresh-queue-display'.")

(defvar org-babel-duckdb--exec-status (make-hash-table :test 'equal)
  "Maps exec-id to execution status for queue display.

Keys are execution ID strings (UUIDs).
Values are symbols: queued, executing, completed, error, cancelled, completed-with-errors.

Updated by `org-babel-duckdb--update-exec-status' via process filter
and completion-handler callbacks.

Only used for queue display. Separate from org-duckdb-blocks tracking
to work without full tracking enabled.

Also see `org-babel-duckdb--get-exec-status'.")

(defvar org-babel-duckdb--exec-names (make-hash-table :test 'equal)
  "Maps exec-id to source block name for queue display.

Keys are execution ID strings (UUIDs).
Values are block name strings from #+NAME: directive, or nil.

Only populated for async executions to enable meaningful queue display.
Entries removed after execution completes.

Updated by `org-babel-execute:duckdb' during async execution start.
Queried by `org-babel-duckdb--refresh-queue-display' for display.

Independent of org-duckdb-blocks tracking system.")

;;;; Queue Management Functions

(defun org-babel-duckdb--session-send-next-request (session)
  "Send commands for next queued request in SESSION if any.

SESSION is session name string.

Retrieves first pending exec-id from queue, updates status to 'executing,
and sends queued commands to session process.

Called by `org-babel-duckdb--enqueue-execution' for first execution and
by `org-babel-duckdb--dequeue-execution' after completion.

Also see `org-babel-duckdb--session-output-filter' for completion detection."
  (when-let* ((queue-info (gethash session org-babel-duckdb--session-queues))
              (pending (plist-get queue-info :pending))
              (next-exec-id (car pending))
              (send-data (gethash next-exec-id
                                  (plist-get queue-info :send-data))))
    ;; Update status
    (org-babel-duckdb--update-exec-status next-exec-id 'executing)

    ;; Get session process
    (when-let* ((session-buffer (gethash session org-babel-duckdb-sessions))
                (process (and (buffer-live-p session-buffer)
                              (get-buffer-process session-buffer))))
      ;; Fire hook for process start
      (run-hook-with-args 'org-babel-duckdb-async-process-started-functions
                          next-exec-id process)
      
      ;; Send the queued commands
      (process-send-string process send-data))))

(defun org-babel-duckdb--maybe-show-queue (session)
  "Auto-show queue for SESSION if conditions met.

SESSION is session name string.

Shows queue when:
- `org-babel-duckdb-queue-display' is 'auto
- Queue has 2+ pending executions
- Queue monitor not already visible

Called by `org-babel-duckdb--enqueue-execution' after adding to queue.

Also see `org-babel-duckdb-show-queue' for manual display."
  (when (and (eq org-babel-duckdb-queue-display 'auto)
             (not org-babel-duckdb--queue-buffer))
    (when-let ((queue-info (gethash session org-babel-duckdb--session-queues)))
      (let ((pending (plist-get queue-info :pending)))
        (when (>= (length pending) 2)
          (org-babel-duckdb-show-queue session))))))

(defun org-babel-duckdb--enqueue-execution (session exec-id completion-handler send-data temp-err-file)
  "Add execution EXEC-ID to SESSION queue.

SESSION is session name string.
EXEC-ID is UUID for this execution.
COMPLETION-HANDLER is callback function invoked when query completes.
SEND-DATA is string of DuckDB commands to send when execution becomes active.
TEMP-ERR-FILE is path to file for capturing stderr output.

Implements FIFO queue: only first pending execution runs at a time.
Subsequent executions wait until previous completes.

Installs cooperative filter on first execution via
`org-babel-duckdb--session-output-filter'.

Sends commands immediately if queue was empty, otherwise queues for later.

Updates status to 'executing for first execution, 'queued for others.

Auto-shows queue monitor if `org-babel-duckdb-queue-display' is 'auto.

Called by `org-babel-duckdb-execute-async'.

Also see `org-babel-duckdb--dequeue-execution' for completion handling and
`org-babel-duckdb--session-send-next-request' for command sending."
  (let* ((queue-info (or (gethash session org-babel-duckdb--session-queues)
                         (let ((info (list :pending nil
                                           :completion-handlers (make-hash-table :test 'equal)
                                           :send-data (make-hash-table :test 'equal)
                                           :temp-err-files (make-hash-table :test 'equal)
                                           :original-filter nil)))
                           (puthash session info org-babel-duckdb--session-queues)
                           info)))
         (pending (plist-get queue-info :pending))
         (completion-handlers (plist-get queue-info :completion-handlers))
         (send-data-table (plist-get queue-info :send-data))
         (temp-err-files (plist-get queue-info :temp-err-files))
         (session-buffer (gethash session org-babel-duckdb-sessions))
         (process (and session-buffer (get-buffer-process session-buffer))))

    ;; Store completion-handler, send-data, and temp-err-file
    (puthash exec-id completion-handler completion-handlers)
    (puthash exec-id send-data send-data-table)
    (puthash exec-id temp-err-file temp-err-files)
    
    ;; Add to pending queue
    (setq pending (append pending (list exec-id)))
    (plist-put queue-info :pending pending)

    ;; Install cooperative filter on first execution only
    (when (and process (= (length pending) 1))
      (plist-put queue-info :original-filter (process-filter process))
      (set-process-filter process
                          (org-babel-duckdb--session-output-filter session)))
    
    ;; If this is the only request, send it immediately
    (when (= (length pending) 1)
      (org-babel-duckdb--session-send-next-request session))
    
    ;; Mark status based on position in queue
    (if (= (length pending) 1)
        (org-babel-duckdb--update-exec-status exec-id 'executing)
      (org-babel-duckdb--update-exec-status exec-id 'queued))
    
    ;; Auto-show queue if configured
    (org-babel-duckdb--maybe-show-queue session)))

(defun org-babel-duckdb--dequeue-execution (session exec-id)
  "Remove EXEC-ID from SESSION queue after completion.

SESSION is session name string.
EXEC-ID is completed execution UUID.

Updates queue structure in `org-babel-duckdb--session-queues'.
Removes completion-handler, send-data, and temp-err-file for completed execution.

Triggers next execution via `org-babel-duckdb--session-send-next-request'
if queue not empty. Skips cancelled executions automatically.

Called by `org-babel-duckdb--session-output-filter' when completion
marker detected.

Also see `org-babel-duckdb--enqueue-execution' for queue initialization."
  (when-let ((queue-info (gethash session org-babel-duckdb--session-queues)))
    (let ((pending (plist-get queue-info :pending))
          (completion-handlers (plist-get queue-info :completion-handlers))
          (send-data-table (plist-get queue-info :send-data))
          (temp-err-files (plist-get queue-info :temp-err-files)))

      ;; Remove from pending list
      (setq pending (delq exec-id pending))
      (plist-put queue-info :pending pending)

      ;; Remove completion-handler, send-data, and temp-err-file
      (remhash exec-id completion-handlers)
      (remhash exec-id send-data-table)
      (remhash exec-id temp-err-files)

      ;; Update queue info
      (puthash session queue-info org-babel-duckdb--session-queues)

      ;; Process next in queue, skipping cancelled
      (when pending
        (let ((next-exec-id (car pending)))
          ;; Check if next execution was cancelled
          (if (eq (org-babel-duckdb--get-exec-status next-exec-id) 'cancelled)
              (progn
                ;; Skip cancelled execution, dequeue and continue
                (message "[ob-duckdb] Skipping cancelled execution %s"
                         (substring next-exec-id 0 8))
                ;; Recursively dequeue cancelled item
                (org-babel-duckdb--dequeue-execution session next-exec-id))
            
            ;; Not cancelled, send normally
            (org-babel-duckdb--session-send-next-request session)))))))

(defun org-babel-duckdb--update-exec-status (exec-id status)
  "Update execution status for EXEC-ID to STATUS.

EXEC-ID is UUID string.
STATUS is symbol: queued, executing, completed, error, cancelled, completed-with-errors.

Stores in `org-babel-duckdb--exec-status' for queue display.

Called by process filter when execution starts and by completion-handler
when execution completes.

Also see `org-babel-duckdb--get-exec-status'."
  (puthash exec-id status org-babel-duckdb--exec-status))

(defun org-babel-duckdb--get-exec-status (exec-id)
  "Get current status for EXEC-ID.

EXEC-ID is UUID string.

Returns status symbol or `queued if not found.

Also see `org-babel-duckdb--update-exec-status' for status values."
  (or (gethash exec-id org-babel-duckdb--exec-status) 'queued))

(defun org-babel-duckdb--refresh-queue-display ()
  "Refresh queue monitor buffer with current execution states.

Updates `org-babel-duckdb--queue-buffer' with latest queue status.
Shows execution states: queued, executing, completed, error, cancelled, completed-with-errors.

Called by `org-babel-duckdb--queue-refresh-timer' every 0.5 seconds.

Stops refresh timer and cleans up when all queues are empty.
For auto-display mode, also closes buffer when queue empties.

Also see `org-babel-duckdb-show-queue' for buffer creation and
`org-babel-duckdb--stop-queue-monitor' for cleanup."
  (when (and org-babel-duckdb--queue-buffer
             (buffer-live-p org-babel-duckdb--queue-buffer))
    (with-current-buffer org-babel-duckdb--queue-buffer
      (let ((inhibit-read-only t)
            (point-before (point)))
        (erase-buffer)
        (insert "DuckDB Async Execution Queues\n")
        (insert "==============================\n")
        (insert (format "Last updated: %s\n\n"
                        (format-time-string "%H:%M:%S")))

        (let ((has-pending nil))
          (maphash
           (lambda (session queue-info)
             (let ((pending (plist-get queue-info :pending)))
               (when pending
                 (setq has-pending t)
                 (insert (format "Session: %s\n" session))
                 (insert (format "  Pending: %d execution(s)\n" (length pending)))
                 (insert "  Queue:\n")
                 (cl-loop for exec-id in pending
                          for idx from 1
                          for status = (org-babel-duckdb--get-exec-status exec-id)
                          for name = (gethash exec-id org-babel-duckdb--exec-names)
                          for status-str = (pcase status
                                             ('executing "executing")
                                             ('completed "completed")
                                             ('error "error")
                                             ('cancelled "cancelled")
                                             ('completed-with-errors "completed-with-errors")
                                             ('queued "")
                                             (_ ""))
                          do (insert (format "    %d. %s%s%s\n"
                                             idx
                                             (substring exec-id 0 8)
                                             (if name (format " %s" name) "")
                                             (if (string-empty-p status-str)
                                                 ""
                                               (format " (%s)" status-str)))))
                 (insert "\n"))))
           org-babel-duckdb--session-queues)

          (unless has-pending
            (if (eq org-babel-duckdb-queue-display 'auto)
                (progn
                  ;; Auto mode: close window and buffer
                  (org-babel-duckdb--stop-queue-monitor)
                  (when (buffer-live-p org-babel-duckdb--queue-buffer)
                    (when-let ((win (get-buffer-window org-babel-duckdb--queue-buffer)))
                      (delete-window win))
                    (kill-buffer org-babel-duckdb--queue-buffer)))
              ;; Manual mode: just stop updating
              (insert "No active async executions.\n")
              (org-babel-duckdb--stop-queue-monitor))))

        ;; Restore point position
        (goto-char (min point-before (point-max)))))))

(defun org-babel-duckdb--stop-queue-monitor ()
  "Stop queue monitor and cleanup resources.

Cancels refresh timer, clears queue buffer reference, and removes
`kill-buffer' hook.

Called automatically when all queues complete or buffer is killed.

Also see `org-babel-duckdb-show-queue' for monitor startup and
`org-babel-duckdb--refresh-queue-display' for refresh logic."
  (when org-babel-duckdb--queue-refresh-timer
    (cancel-timer org-babel-duckdb--queue-refresh-timer)
    (setq org-babel-duckdb--queue-refresh-timer nil))
  (setq org-babel-duckdb--queue-buffer nil))

;;;###autoload
(defun org-babel-duckdb-show-queue (&optional session)
  "Display pending async executions for SESSION with live updates.

When SESSION is nil, shows queues for all sessions.

Creates live-updating buffer that refreshes every 0.5 seconds.
Buffer shows execution states: queued, executing, completed, error, cancelled, completed-with-errors.

Display position controlled by `org-babel-duckdb-queue-position':
  bottom - Bottom side window (25% height)
  side   - Right side window (25% width)

Display format:
  Session: SESSION-NAME
    Pending: N execution(s)
    Queue:
      1. EXEC-ID (status)
      2. EXEC-ID (status)
      ...

Status values:
- (executing) - Currently running
- (completed) - Completed successfully
- (error) - Failed with error
- (cancelled) - User cancelled
- (completed-with-errors) - Completed with warnings
- No label - Queued, waiting to execute

Auto-stops when all queues complete.
Press 'q' to close buffer and stop monitoring.
Press 'g' to force refresh.

Information from `org-babel-duckdb--session-queues' and
`org-babel-duckdb--exec-status'.

For detailed execution info, enable `org-duckdb-blocks-enable-tracking'
and use `org-duckdb-blocks-show-execution-status'.

Also see `org-babel-duckdb-queue-display' for auto-display settings."
  (interactive
   (list (when (> (hash-table-count org-babel-duckdb-sessions) 1)
           (completing-read "Session (blank for all): "
                            (hash-table-keys org-babel-duckdb-sessions)
                            nil t nil nil ""))))

  ;; Stop existing monitor if running
  (when org-babel-duckdb--queue-refresh-timer
    (org-babel-duckdb--stop-queue-monitor))

  ;; Create or reuse buffer
  (let ((buf (get-buffer-create "*DuckDB Async Queue*")))
    (setq org-babel-duckdb--queue-buffer buf)

    (with-current-buffer buf
      (setq buffer-read-only t)
      (use-local-map (make-sparse-keymap))
      (local-set-key (kbd "q")
                     (lambda ()
                       (interactive)
                       (org-babel-duckdb--stop-queue-monitor)
                       (quit-window t)))
      (local-set-key (kbd "g")
                     (lambda ()
                       (interactive)
                       (org-babel-duckdb--refresh-queue-display)))

      ;; Add kill-buffer hook for cleanup
      (add-hook 'kill-buffer-hook
                #'org-babel-duckdb--stop-queue-monitor
                nil t))

    ;; Initial display
    (org-babel-duckdb--refresh-queue-display)

    ;; Start refresh timer
    (setq org-babel-duckdb--queue-refresh-timer
          (run-with-timer 0.5 0.5 #'org-babel-duckdb--refresh-queue-display))

    ;; Display buffer with position from customization
    (display-buffer buf
                    (pcase org-babel-duckdb-queue-position
                      ('bottom
                       '(display-buffer-in-side-window
                         (side . bottom)
                         (window-height . 0.25)))
                      ('side
                       '(display-buffer-in-side-window
                         (side . right)
                         (window-width . 0.25)))
                      (_
                       '(display-buffer-in-side-window
                         (side . bottom)
                         (window-height . 0.25)))))))

;;;; Session Management Functions

(defun org-babel-duckdb--session-output-filter (session)
  "Create output filter for SESSION handling FIFO async execution queue.

SESSION is session name string.

Returns filter function suitable for `set-process-filter'.

The filter implements strict FIFO execution:
1. Captures errors to temp-err-file for current execution
2. Updates progress display for current execution
3. Checks only first pending execution for completion marker
4. Invokes completion-handler when completion marker found
5. Triggers next execution via `org-babel-duckdb--dequeue-execution'
6. Forwards output to original filter if exists
7. Restores original filter when queue empties

Installed by `org-babel-duckdb--enqueue-execution' on first execution.
Removed automatically when queue becomes empty.

Also see `org-babel-duckdb--session-send-next-request' for command sending
and `org-babel-duckdb-execute-async' for async execution setup."
  (lambda (proc string)
    (when-let* ((queue-info (gethash session org-babel-duckdb--session-queues))
                (pending (plist-get queue-info :pending))
                (current-exec-id (car pending)))  ; Only check FIRST pending

      ;; Capture errors to temp-err-file
      (when (string-match-p "\\(Error:\\|Exception:\\|SYNTAX_ERROR\\|CATALOG_ERROR\\|BINDER_ERROR\\|PARSER_ERROR\\)" string)
        (when-let ((temp-err-file (gethash current-exec-id 
                                           (plist-get queue-info :temp-err-files))))
          (with-temp-file temp-err-file
            (when (file-exists-p temp-err-file)
              (insert-file-contents temp-err-file))
            (goto-char (point-max))
            (insert (format "[%s] %s\n"
                            (format-time-string "%H:%M:%S")
                            (string-trim string))))))

      ;; Update progress for current execution only
      (when (gethash current-exec-id org-babel-duckdb-progress-buffers)
        (org-babel-duckdb-update-progress-display current-exec-id string))

      ;; Check for completion marker of CURRENT execution only
      (let ((marker (format "ASYNC_COMPLETE_%s" current-exec-id)))
        (when (string-search marker string)

          ;; Mark as completed
          (org-babel-duckdb--update-exec-status current-exec-id 'completed)

          ;; Invoke completion-handler
          (when-let ((completion-handler (gethash current-exec-id
                                                  (plist-get queue-info :completion-handlers))))
            (funcall completion-handler proc "finished\n"))

          ;; Remove from queue (this will trigger next send)
          (org-babel-duckdb--dequeue-execution session current-exec-id))))

    ;; Forward to original filter if exists
    (when-let ((original-filter (plist-get
                                 (gethash session org-babel-duckdb--session-queues)
                                 :original-filter)))
      (funcall original-filter proc string))

    ;; Restore original filter if queue empty
    (when-let* ((queue-info (gethash session org-babel-duckdb--session-queues))
                (empty-p (null (plist-get queue-info :pending))))
      (when-let ((original-filter (plist-get queue-info :original-filter)))
        (set-process-filter proc original-filter))
      (remhash session org-babel-duckdb--session-queues))))

(defun org-babel-duckdb-get-session-buffer (session-name)
  "Get or create buffer for SESSION-NAME.

SESSION-NAME is session name string.

Buffer name follows pattern *DuckDB:SESSION-NAME*.
Registers new buffers in `org-babel-duckdb-sessions'.

Called by `org-babel-duckdb-initiate-session'.
Also see `org-babel-duckdb-list-sessions'."
  (let ((buffer-name (format "*DuckDB:%s*" session-name)))
    (or (gethash session-name org-babel-duckdb-sessions)
        (let ((new-buffer (get-buffer-create buffer-name)))
          (puthash session-name new-buffer org-babel-duckdb-sessions)
          new-buffer))))

;;;###autoload
(defun org-babel-duckdb-cleanup-sessions ()
  "Remove dead sessions from the registry.

A session is dead if its buffer or process no longer exists.

Updates `org-babel-duckdb-sessions'.
Called automatically by `org-babel-duckdb-delete-session'.
Also see `org-babel-duckdb-list-sessions' and
`org-babel-duckdb-display-sessions'."
  (interactive)
  (maphash
   (lambda (name buffer)
     (when (or (not (buffer-live-p buffer))
               (not (process-live-p (get-buffer-process buffer))))
       (remhash name org-babel-duckdb-sessions)))
   org-babel-duckdb-sessions))

(defun org-babel-duckdb-initiate-session (&optional session-name params)
  "Create or reuse DuckDB session SESSION-NAME with PARAMS.

If session exists and process is alive, return existing buffer.
Otherwise, create new session with `comint-mode' and start DuckDB process.

SESSION-NAME of nil or \"yes\" becomes \"default\".
SESSION-NAME of \"none\" returns nil (no session).

PARAMS alist may include :db for database file path (see
`org-babel-duckdb-command').

Returns session buffer or nil.

New sessions are registered in `org-babel-duckdb-sessions'.
Uses `org-babel-duckdb-get-session-buffer' for buffer creation.
Prompt configured via `org-babel-duckdb-prompt-char'.
Database file stored in buffer-local `org-babel-duckdb-session-db-file'.

For session management, see `org-babel-duckdb-delete-session' and
`org-babel-duckdb-cleanup-sessions'."
  (unless (string= session-name "none")
    (let* ((session-name (if (or (null session-name) (string= session-name "yes"))
                             "default" session-name))
           (buffer (org-babel-duckdb-get-session-buffer session-name))
           (db-file (cdr (assq :db params)))
           (process (and (buffer-live-p buffer) (get-buffer-process buffer))))

      (unless (and process (process-live-p process))
        (with-current-buffer buffer
          (erase-buffer)

          (unless (derived-mode-p 'comint-mode)
            (comint-mode)
            (setq-local comint-prompt-regexp "^D> ")
            (setq-local comint-process-echoes t))

          ;; Store database file as buffer-local variable
          (setq-local org-babel-duckdb-session-db-file db-file)

          (let* ((cmd-args (list org-babel-duckdb-command))
                 (cmd-args (if db-file (append cmd-args (list db-file)) cmd-args)))

            (comint-exec buffer
                         (format "duckdb-%s" session-name)
                         (car cmd-args)
                         nil
                         (cdr cmd-args))

            (setq process (get-buffer-process buffer))

            (unless (and process (process-live-p process))
              (error "Failed to start DuckDB process for session %s" session-name))

            (set-process-coding-system process 'utf-8 'utf-8)

            (let ((timeout 10)
                  (waited 0)
                  (ready nil)
                  (last-pos (point-min)))
              (while (and (< waited timeout) (not ready))
                (accept-process-output process 0.2)
                (setq waited (+ waited 0.2))

                (save-excursion
                  (goto-char last-pos)
                  (when (re-search-forward "\\(^D> \\|v[0-9]+\\.[0-9]+\\.[0-9]+\\|Enter \"\\.\\)" nil t)
                    (setq ready t)
                    (setq last-pos (point)))))

              (unless ready
                (kill-process process)
                (error "DuckDB session %s failed to initialize within %d seconds"
                       session-name timeout))))))

      buffer)))

;;;; Variable Handling Functions

(defun org-babel-expand-body:duckdb (body params)
  "Expand BODY with variables from PARAMS, adding prologue and epilogue.

Variable substitution supports three patterns:
1. varname[key]  - Access table cell by key
2. $varname      - Direct substitution
3. varname       - Word-boundary matched substitution

Also prepends :prologue and appends :epilogue if present in PARAMS.

Uses direct text replacement for value conversion.
Called by `org-babel-execute:duckdb' before execution.

See Info node `(org) Environment of a Code Block' for variable binding."
  (let ((prologue (cdr (assq :prologue params)))
        (epilogue (cdr (assq :epilogue params))))
    (with-temp-buffer
      (insert body)

      (let ((vars (org-babel--get-vars params)))
        (dolist (pair vars)
          (let ((name (car pair))
                (value (cdr pair)))

            (goto-char (point-min))
            (while (re-search-forward
                    (format "\\(?:\\$%s\\|\\b%s\\[\\([^]]+\\)\\]\\|\\b%s\\b\\)"
                            (regexp-quote (symbol-name name))
                            (regexp-quote (symbol-name name))
                            (regexp-quote (symbol-name name)))
                    nil t)
              (cond
               ((match-beginning 1)
                (let* ((key (match-string 1))
                       (cell-value nil))
                  (when (and (listp value) (> (length value) 0))
                    (dolist (row value)
                      (when (and (listp row) (>= (length row) 2)
                                 (equal (car row) key))
                        (setq cell-value (cadr row)))))
                  (when cell-value
                    (replace-match (if (stringp cell-value)
                                       cell-value
                                     (format "%S" cell-value))
                                   t t))))

               ((eq (char-before (match-beginning 0)) ?$)
                (replace-match (if (stringp value)
                                   value
                                 (format "%S" value))
                               t t))

               (t
                (unless (looking-back "\\[" 1)
                  (replace-match (if (stringp value)
                                     value
                                   (format "%S" value))
                                 t t))))))))

      (setq body (buffer-string)))

    (mapconcat #'identity
               (delq nil (list prologue body epilogue))
               "\n")))

;;;; Command Processing Functions

(defun org-babel-duckdb-process-params (params)
  "Generate DuckDB dot commands from PARAMS header arguments.

Converts header arguments to DuckDB configuration commands.
Returns string of newline-separated dot commands, or nil if no
recognized parameters present.

Parameters processed: :format, :timer, :headers, :nullvalue, :separator,
:echo, :bail. See `org-babel-header-args:duckdb' for specifications.

Prompt is always set to `org-babel-duckdb-prompt-char'.

Called by `org-babel-execute:duckdb' before execution."
  (let ((format (cdr (assq :format params)))
        (timer (cdr (assq :timer params)))
        (headers (cdr (assq :headers params)))
        (nullvalue (cdr (assq :nullvalue params)))
        (separator (cdr (assq :separator params)))
        (echo (cdr (assq :echo params)))
        (bail (cdr (assq :bail params))))

    (with-temp-buffer
      (insert (format ".prompt %s\n" org-babel-duckdb-prompt-char))
      (when format    (insert (format ".mode %s\n"      format)))
      (when nullvalue (insert (format ".nullvalue %s\n" nullvalue)))
      (when separator (insert (format ".separator %s\n" separator)))
      (when timer     (insert (format ".timer %s\n"     (if (string= timer   "off") "off" "on"))))
      (when headers   (insert (format ".headers %s\n"   (if (string= headers "off") "off" "on"))))
      (when echo      (insert (format ".echo %s\n"      (if (string= echo    "off") "off" "on"))))
      (when bail      (insert (format ".bail %s\n"      (if (string= bail    "off") "off" "on"))))

      (when (> (buffer-size) 0)
        (buffer-string)))))

(defun org-babel-duckdb-write-temp-sql (body)
  "Write SQL BODY to temporary file and return filename.

Uses temp files to avoid command-line length limits and preserve
formatting for multi-statement queries.

File managed by Org's temporary file system.
Used by `org-babel-duckdb-execute-sync' and
`org-babel-duckdb-execute-async'."
  (let ((temp-file (org-babel-temp-file "duckdb-")))
    (with-temp-file temp-file
      (insert body))
    temp-file))

(defun org-babel-duckdb-read-file-lines (file max-lines)
  "Read up to MAX-LINES from FILE, return (content . truncated-p).

Efficiently handles large files without loading entire contents into memory.
MAX-LINES of nil or 0 means read entire file.

Used by `org-babel-duckdb-execute-sync' and `org-babel-duckdb--make-completion-handler'
to apply truncation from `org-babel-duckdb-max-rows'."
  (if (or (not max-lines) (<= max-lines 0))
      (cons (with-temp-buffer
              (insert-file-contents file)
              (buffer-string))
            nil)

    (with-temp-buffer
      (insert-file-contents file)
      (goto-char (point-min))
      (let ((lines-kept 0)
            (truncated nil))

        (while (and (< lines-kept max-lines)
                    (not (eobp)))
          (forward-line 1)
          (setq lines-kept (1+ lines-kept)))

        (unless (eobp)
          (setq truncated t)
          (delete-region (point) (point-max)))

        (cons (buffer-string) truncated)))))

(defun org-babel-duckdb-get-max-rows (params)
  "Determine max rows setting from PARAMS or default.

Returns integer limit or nil for unlimited.
PARAMS :max-rows can be nil, \"nil\" (unlimited), or numeric.

Consults `org-babel-duckdb-max-rows' as default.
Used by `org-babel-duckdb-read-file-lines' and
`org-babel-duckdb-execute-session'."
  (let ((max-rows-param (cdr (assq :max-rows params))))
    (cond
     ((null max-rows-param) org-babel-duckdb-max-rows)
     ((and (stringp max-rows-param) (string= max-rows-param "nil")) nil)
     ((numberp max-rows-param) max-rows-param)
     ((stringp max-rows-param) (string-to-number max-rows-param))
     (t org-babel-duckdb-max-rows))))

(defun org-babel-duckdb-clean-output (output)
  "Remove prompt characters from first line of OUTPUT.

Minimal cleanup - only strips session prompt from command echo line.
Does not process ANSI colors or remove progress indicators.

Prompt character from `org-babel-duckdb-prompt-char'.
Called by `org-babel-duckdb--insert-result-at-point'."
  (if (string-empty-p output)
      output
    (let ((prompt-char (regexp-quote org-babel-duckdb-prompt-char))
          (newline-pos (string-search "\n" output)))
      (if newline-pos
          (let ((first-line (substring output 0 newline-pos))
                (rest (substring output newline-pos)))
            (concat (replace-regexp-in-string prompt-char "" first-line) rest))
        (replace-regexp-in-string prompt-char "" output)))))

;;;; Execution Functions

;;;;; Synchronous Execution

(defun org-babel-duckdb-execute-session (session-buffer body params)
  "Execute SQL BODY in SESSION-BUFFER with PARAMS, blocking until complete.

SESSION-BUFFER must contain a live DuckDB process (see
`org-babel-duckdb-initiate-session').

BODY is SQL code as a string.
PARAMS is alist of header arguments.

Returns output string, possibly truncated per :max-rows (see
`org-babel-duckdb-get-max-rows').

Blocks Emacs until execution finishes or times out (60 seconds).

For non-blocking execution, use `org-babel-duckdb-execute-async'.
Also see `org-babel-duckdb-process-params' for dot command generation."
  (let* ((dot-commands (org-babel-duckdb-process-params params))
         (full-body (if dot-commands
                        (concat dot-commands "\n" body)
                      body))
         (process (get-buffer-process session-buffer))
         (completion-marker "DUCKDB_QUERY_COMPLETE_MARKER")
         (max-rows (org-babel-duckdb-get-max-rows params)))

    (with-current-buffer session-buffer
      (goto-char (point-max))
      (let ((start-point (point)))
        (when (> start-point (line-beginning-position))
          (process-send-string process "\n")
          (accept-process-output process 0.01))

        (goto-char (point-max))
        (setq start-point (point))

        (process-send-string process
                             (concat full-body
                                     "\n.print \"" completion-marker "\"\n"))

        (let ((found nil)
              (timeout 60)
              (wait-time 0.05)
              (total-waited 0))

          (while (and (not found) (< total-waited timeout))
            (accept-process-output process wait-time)
            (setq total-waited (+ total-waited wait-time))

            (save-excursion
              (goto-char start-point)
              (when (search-forward completion-marker nil t)
                (setq found t)))

            (cond
             ((< total-waited 1) (setq wait-time 0.05))
             ((< total-waited 5) (setq wait-time 0.2))
             (t (setq wait-time 0.5))))

          (if found
              (let* ((marker-pos (save-excursion
                                   (goto-char start-point)
                                   (search-forward completion-marker nil t)))
                     (output-end (when marker-pos (line-beginning-position 0)))
                     (raw-output (buffer-substring-no-properties
                                  start-point output-end)))

                (if (and max-rows (> max-rows 0))
                    (let* ((lines (split-string raw-output "\n"))
                           (line-count (length lines)))
                      (if (> line-count max-rows)
                          (concat (string-join (seq-take lines max-rows) "\n")
                                  (format "\n\n[Output truncated to %d lines - customize org-babel-duckdb-max-rows to change]"
                                          max-rows))
                        raw-output))
                  raw-output))

            (error "DuckDB query timed out after %d seconds" timeout)))))))

(defun org-babel-duckdb-execute-sync (session body params)
  "Execute BODY synchronously in SESSION or directly.

SESSION is nil/\"none\" for direct execution, or session name string.
BODY is SQL code to execute.
PARAMS is alist of header arguments.

Returns output string, possibly truncated (see `org-babel-duckdb-get-max-rows').

Direct mode invokes `org-babel-duckdb-command' as subprocess with file I/O.
Session mode delegates to `org-babel-duckdb-execute-session'.

Uses `org-babel-duckdb-write-temp-sql' for input file creation and
`org-babel-duckdb-read-file-lines' for result capture with truncation.

For async execution, see `org-babel-duckdb-execute-async'."
  (let* ((use-session-p (and session (not (string= session "none"))))
         (db-file (cdr (assq :db params)))
         (temp-in-file (org-babel-duckdb-write-temp-sql body))
         (temp-out-file (org-babel-temp-file "duckdb-out-"))
         (max-rows (org-babel-duckdb-get-max-rows params)))

    (if use-session-p
        (let ((session-buffer (org-babel-duckdb-initiate-session session params)))
          (org-babel-duckdb-execute-session session-buffer
                                            (format ".read %s" temp-in-file)
                                            params))

      (let ((command (format "%s %s -init /dev/null -batch < %s > %s"
                             org-babel-duckdb-command
                             (or db-file "")
                             temp-in-file
                             temp-out-file)))
        (org-babel-eval command "")

        (let ((result (org-babel-duckdb-read-file-lines temp-out-file max-rows)))
          (if (cdr result)
              (concat (car result)
                      (format "\n\n[Output truncated to %d lines - customize org-babel-duckdb-max-rows to change]"
                              max-rows))
            (car result)))))))

;;;;; Asynchronous Execution

(defun org-babel-duckdb--find-result-location (exec-id)
  "Find result insertion location for async execution EXEC-ID.

EXEC-ID is UUID string.

Returns cons cell (BUFFER . MARKER) or nil if not found.

Search strategy:
1. Check `org-babel-duckdb--pending-async' for stored location
2. Scan current buffer for placeholder containing exec-id

The embedded exec-id in placeholder text enables routing even after
document edits, as markers move with text.

Used by `org-babel-duckdb--make-completion-handler' to route results.

Also see `org-babel-execute:duckdb' for placeholder insertion."
  (or
   ;; Fast path: stored location
   (gethash exec-id org-babel-duckdb--pending-async)

   ;; Fallback: scan buffer for placeholder
   (save-excursion
     (goto-char (point-min))
     (when (re-search-forward
            (format "Execution ID: %s" (regexp-quote exec-id))
            nil t)
       (cons (current-buffer) (point-marker))))))

(defun org-babel-duckdb-execute-async (session body params exec-id)
  "Execute BODY asynchronously in SESSION without blocking Emacs.

BODY is SQL to execute.
SESSION is session name (required for async).
PARAMS is alist of header arguments.
EXEC-ID is UUID for this execution.

Returns process object.

Execution uses file-based output redirection for performance with large
results. Completion detected via unique marker in session output.

Implements FIFO queue via `org-babel-duckdb--enqueue-execution':
only one execution runs at a time per session, others wait in queue.

Results routed via `org-babel-duckdb--find-result-location' which
handles document edits during execution.

Progress monitoring controlled by `org-babel-duckdb-show-progress' and
`org-babel-duckdb-progress-display'.

To cancel, use `org-babel-duckdb-cancel-execution'.
For synchronous execution, see `org-babel-duckdb-execute-sync'."
  (let* ((session-buffer (org-babel-duckdb-initiate-session session params))
         (process (get-buffer-process session-buffer))
         (temp-out-file (org-babel-temp-file "duckdb-async-out-"))
         (temp-err-file (org-babel-temp-file "duckdb-async-err-"))
         (temp-script-file (org-babel-duckdb-write-temp-sql body))
         (result-params (cdr (assq :result-params params)))
         (completion-marker (format "ASYNC_COMPLETE_%s" exec-id)))

    (when org-babel-duckdb-show-progress
      (org-babel-duckdb-create-progress-monitor exec-id session-buffer))

    ;; Create completion-handler
    (let ((completion-handler (org-babel-duckdb--make-completion-handler
                               exec-id temp-out-file temp-err-file temp-script-file
                               params result-params)))

      ;; Prepare send data (don't send yet - queue will send when ready)
      (let ((send-data
             (concat
              ".bail on\n"
              (format ".output %s\n" (expand-file-name temp-out-file))
              (format ".read %s\n" (expand-file-name temp-script-file))
              ".output\n"
              (format ".print \"%s\"\n" completion-marker))))

        ;; Enqueue with send-data AND temp-err-file
        (org-babel-duckdb--enqueue-execution session exec-id completion-handler send-data temp-err-file)))

    process))

(defun org-babel-duckdb--make-completion-handler (exec-id temp-out-file temp-err-file temp-script-file params result-params)
  "Create completion handler for async execution EXEC-ID.

Returns callback function invoked when execution completes.
Reads output from TEMP-OUT-FILE, classifies status, updates results,
and cleans up temporary files.

TEMP-ERR-FILE and TEMP-SCRIPT-FILE are also cleaned up.
PARAMS and RESULT-PARAMS control result insertion.

Result location resolved via `org-babel-duckdb--find-result-location'
which handles document edits during execution.

Progress cleanup via `org-babel-duckdb-cleanup-progress-display'.
Truncation via `org-babel-duckdb-get-max-rows' and
`org-babel-duckdb-read-file-lines'.

Handles both .bail on (stops on error, non-zero exit) and .bail off
(continues on error, zero exit with both stdout and stderr).

Fires `org-babel-duckdb-execution-completed-functions' hook with
status information.

The handler is registered by `org-babel-duckdb-execute-async' and
invoked by `org-babel-duckdb--session-output-filter' when completion
marker detected."
  (lambda (process event)
    (let ((exit-status (process-exit-status process))
          (status-msg (string-trim event))
          (max-rows (org-babel-duckdb-get-max-rows params))
          (final-status nil)
          (error-info nil))

      (unwind-protect
          (condition-case err
              (let* ((stdout-result (if (file-exists-p temp-out-file)
                                        (org-babel-duckdb-read-file-lines temp-out-file max-rows)
                                      (cons "" nil)))
                     (stdout-content (car stdout-result))
                     (was-truncated (cdr stdout-result))
                     (stderr-content (if (file-exists-p temp-err-file)
                                         (with-temp-buffer
                                           (insert-file-contents temp-err-file)
                                           (buffer-string))
                                       ""))
                     (has-output (not (string-empty-p (string-trim stdout-content))))
                     (has-errors (not (string-empty-p (string-trim stderr-content)))))

                (when was-truncated
                  (setq stdout-content
                        (concat stdout-content
                                (format "\n\n[Output truncated to %d lines - customize org-babel-duckdb-max-rows to change]"
                                        max-rows))))

                ;; Determine status and error-info
                (cond
                 ;; Non-zero exit: .bail on was active, execution stopped on error
                 ((not (zerop exit-status))
                  (setq final-status 'error)
                  (setq error-info (if has-errors stderr-content
                                     (format "Process exited with code %d" exit-status))))
                 
                 ;; Zero exit with errors: .bail off was active, execution continued
                 ((and (zerop exit-status) has-errors)
                  (setq final-status 'completed-with-errors)
                  (setq error-info (format "Completed with errors: %s" 
                                           (substring stderr-content 0 (min 100 (length stderr-content))))))
                 
                 ;; Zero exit, no errors: clean success
                 ((and (zerop exit-status) (not has-errors))
                  (setq final-status 'completed))
                 
                 ;; Cancelled/interrupted
                 ((string-match-p "\\(killed\\|terminated\\|interrupt\\)" event)
                  (setq final-status 'cancelled)
                  (setq error-info status-msg))
                 
                 ;; Unknown state
                 (t
                  (setq final-status 'unknown)
                  (setq error-info event)))

                ;; Update internal status
                (org-babel-duckdb--update-exec-status exec-id final-status)

                ;; Fire completion hook
                (run-hook-with-args 'org-babel-duckdb-execution-completed-functions
                                    exec-id final-status error-info)

                ;; Insert results at located position
                (when-let ((location (org-babel-duckdb--find-result-location exec-id)))
                  (let ((buffer (car location))
                        (marker (cdr location)))
                    (when (buffer-live-p buffer)
                      (with-current-buffer buffer
                        (save-excursion
                          (goto-char marker)
                          (let ((final-content
                                 (cond
                                  ;; Non-zero exit: .bail on, show only error
                                  ((eq final-status 'error)
                                   (format "DuckDB Error:\n%s" error-info))
                                  
                                  ;; Zero exit with both output and errors: .bail off
                                  ((and (eq final-status 'completed-with-errors) has-output)
                                   (concat stdout-content
                                           "\n\n--- Errors occurred during execution ---\n"
                                           stderr-content))
                                  
                                  ;; Zero exit with only errors (no output): unusual but possible
                                  ((and (eq final-status 'completed-with-errors) (not has-output))
                                   (format "DuckDB Error:\n%s" stderr-content))
                                  
                                  ;; Zero exit with only output: normal success
                                  ((and (eq final-status 'completed) has-output)
                                   stdout-content)
                                  
                                  ;; Zero exit with no output and no errors
                                  ((and (eq final-status 'completed) (not has-output))
                                   "Query completed but produced no output")
                                  
                                  ;; Cancelled/interrupted
                                  ((eq final-status 'cancelled)
                                   (format "Execution cancelled: %s" error-info))
                                  
                                  ;; Unknown state
                                  (t
                                   (format "Unknown process status: %s" error-info)))))
                            
                            (org-babel-remove-result)
                            (org-babel-duckdb--insert-result-at-point 
                             final-content params result-params)))))))

                ;; Clean up async registry
                (remhash exec-id org-babel-duckdb--pending-async)
                (remhash exec-id org-babel-duckdb--exec-names))

            (error
             (let ((error-msg (format "Completion handler error: %S" err)))
               (setq final-status 'error)
               (setq error-info error-msg)
               (org-babel-duckdb--update-exec-status exec-id 'error)
               (run-hook-with-args 'org-babel-duckdb-execution-completed-functions
                                   exec-id 'error error-msg)
               (message "[ob-duckdb] %s" error-msg))))

        (org-babel-duckdb-cleanup-progress-display exec-id)
        (when (file-exists-p temp-out-file) (delete-file temp-out-file))
        (when (file-exists-p temp-err-file) (delete-file temp-err-file))
        (when (file-exists-p temp-script-file) (delete-file temp-script-file))))))

;;;;; Result Processing

(defun org-babel-duckdb--extract-exec-id-from-results ()
  "Extract exec-id from results block at point.

Searches for pattern 'Execution ID: <UUID>' in results block below
current source block.

Returns exec-id string or nil if not found.

Used by `org-babel-duckdb-cancel-block-at-point'.

Also see `org-babel-execute:duckdb' for placeholder format."
  (save-excursion
    (when-let ((result-pos (org-babel-where-is-src-block-result)))
      (goto-char result-pos)
      (when (re-search-forward "Execution ID: \\([a-f0-9-]+\\)"
                               (org-babel-result-end) t)
        (match-string 1)))))

(defun org-babel-duckdb--insert-result-at-point (content params result-params)
  "Insert CONTENT as result at point with PARAMS and RESULT-PARAMS.

Helper function factored out from result insertion logic.
Handles wrapping, ANSI color processing, and table conversion.

CONTENT is result string to insert.
PARAMS is alist of header arguments.
RESULT-PARAMS controls formatting and insertion.

Respects :wrap header argument and :results table directive.
Processes ANSI escape codes for color output.

Called by `org-babel-duckdb--make-completion-handler' and
`org-babel-execute:duckdb'."
  (let* ((cleaned-output (org-babel-duckdb-clean-output content))
         (output-type (cdr (assq :output params)))
         (use-buffer-output (and output-type (string= output-type "buffer"))))

    (when (and (stringp cleaned-output)
               (string-match-p "\e\\[" cleaned-output))
      (with-temp-buffer
        (insert cleaned-output)
        (ansi-color-apply-on-region (point-min) (point-max))
        (setq cleaned-output (buffer-string))))

    (if use-buffer-output
        (progn
          (org-babel-insert-result "Output sent to buffer." result-params)
          (org-babel-duckdb-display-buffer cleaned-output))

      (org-babel-insert-result
       (if (member "table" result-params)
           (org-babel-duckdb-table-or-scalar cleaned-output)
         cleaned-output)
       result-params)

      (when-let ((result-pos (org-babel-where-is-src-block-result)))
        (save-excursion
          (goto-char result-pos)
          (when (looking-at org-babel-result-regexp)
            (let ((end (org-babel-result-end))
                  (ansi-color-context-region nil))
              (ansi-color-apply-on-region result-pos end))))))))

(defun org-babel-duckdb-table-or-scalar (result)
  "Convert RESULT string into Elisp structure for Org tables.

Parses DuckDB text output into (header . data-rows) format.
Only used when :results table is specified.

Returns cons cell or original string if not parseable.

See Info node `(org) Results of Evaluation' for result types."
  (let ((lines (split-string result "\n" t)))
    (if (and (> (length lines) 1)
             (string-match "^[-+|]" (nth 1 lines)))
        (let* ((header (car lines))
               (separator (nth 1 lines))
               (data (cddr lines)))
          (cons (split-string header "|" t)
                (mapcar (lambda (row) (split-string row "|" t)) data)))
      result)))

(defun org-babel-duckdb-display-buffer (output)
  "Display OUTPUT in dedicated buffer `org-babel-duckdb-output-buffer'.

Processes ANSI colors and displays buffer.

Called by `org-babel-duckdb--insert-result-at-point' when :output buffer
is specified in source block header."
  (let ((buf (get-buffer-create org-babel-duckdb-output-buffer)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert output)
        (ansi-color-apply-on-region (point-min) (point-max))
        (goto-char (point-min))))
    (display-buffer buf)
    output))

;;;; Progress Monitoring Functions

(defun org-babel-duckdb-create-progress-monitor (exec-id session-buffer)
  "Create status monitoring for async execution EXEC-ID.

Display method controlled by `org-babel-duckdb-progress-display':
  popup      - Dedicated buffer in side window
  minibuffer - One-time message (delayed to avoid Org message collision)
  nil        - No display

SESSION-BUFFER currently unused.

For popup mode:
- Registers buffer in `org-babel-duckdb-progress-buffers'
- Updates via `org-babel-duckdb-update-progress-display'
- Cleanup via `org-babel-duckdb-cleanup-progress-display'

For minibuffer mode:
- Shows delayed message (0.1s) to appear after Org's completion message
- No ongoing updates

Only called when `org-babel-duckdb-show-progress' is non-nil."
  (pcase org-babel-duckdb-progress-display
    ('popup
     (let* ((progress-buffer-name (format "*DuckDB Progress-%s*" (substring exec-id 0 8)))
            (progress-buffer (get-buffer-create progress-buffer-name)))

       (puthash exec-id progress-buffer org-babel-duckdb-progress-buffers)

       (with-current-buffer progress-buffer
         (erase-buffer)
         (insert (format "DuckDB Execution Progress\nExecution ID: %s\n\n" exec-id))
         (goto-char (point-max)))

       ;; Use popup window instead of side window
       (display-buffer progress-buffer
                       '((display-buffer-pop-up-window)
                         (window-height . 0.3)
                         (window-parameters . ((no-other-window . t)
                                               (no-delete-other-windows . t)))))

       progress-buffer))

    ('minibuffer
     ;; Delay message to appear after Org's "Code block evaluation complete"
     (run-with-timer 0.1 nil
                     (lambda ()
                       (message "[DuckDB] Executing async query %s..."
                                (substring exec-id 0 8))))
     nil)

    (_ nil)))

(defun org-babel-duckdb-update-progress-display (exec-id output)
  "Update status display for EXEC-ID with new OUTPUT from process.

Filters OUTPUT for status lines and appends with timestamps.

Called by async execution filter installed by
`org-babel-duckdb--session-output-filter'.

Updates buffer in `org-babel-duckdb-progress-buffers'."
  (when-let ((progress-buffer (gethash exec-id org-babel-duckdb-progress-buffers)))
    (when (buffer-live-p progress-buffer)
      (with-current-buffer progress-buffer
        (goto-char (point-max))
        (let ((progress-lines (cl-remove-if-not
                               (lambda (line)
                                 (string-match-p "\\([0-9]+%\\|▕\\)" line))
                               (split-string output "\n"))))
          (dolist (line progress-lines)
            (when (not (string-empty-p line))
              (insert (format "[%s] %s\n"
                              (format-time-string "%H:%M:%S")
                              (string-trim line)))))
          (goto-char (point-max)))))))

(defun org-babel-duckdb-cleanup-progress-display (exec-id)
  "Clean up status display for EXEC-ID after execution completes.

Behavior depends on `org-babel-duckdb-progress-display':
  popup      - Append completion message, schedule buffer kill after 3s
  minibuffer - Show delayed completion message
  nil        - No action

Removes EXEC-ID from `org-babel-duckdb-progress-buffers'.

Called by `org-babel-duckdb--make-completion-handler'."
  (pcase org-babel-duckdb-progress-display
    ('popup
     (when-let ((progress-buffer (gethash exec-id org-babel-duckdb-progress-buffers)))
       (when (buffer-live-p progress-buffer)
         (with-current-buffer progress-buffer
           (goto-char (point-max))
           (insert (format "\n[%s] Execution completed.\n"
                           (format-time-string "%H:%M:%S"))))
         
         ;; Close window and kill buffer after delay
         (run-with-timer 3.0 nil
                         (lambda (buf)
                           (when (buffer-live-p buf)
                             (when-let ((win (get-buffer-window buf)))
                               (delete-window win))
                             (kill-buffer buf)))
                         progress-buffer))
       (remhash exec-id org-babel-duckdb-progress-buffers)))

    ('minibuffer
     ;; Delay message to avoid collision with completion-handler messages
     (run-with-timer 0.1 nil
                     (lambda ()
                       (message "[DuckDB] Async query %s completed"
                                (substring exec-id 0 8)))))

    (_ nil)))

;;;; Interactive Session Management Commands

;;;###autoload
(defun org-babel-duckdb-list-sessions ()
  "List all active DuckDB sessions.

When interactive, displays in minibuffer.
From Lisp, returns alist of (NAME . BUFFER) pairs.

Queries `org-babel-duckdb-sessions'.
Also see `org-babel-duckdb-display-sessions' for detailed view."
  (interactive)
  (let (sessions)
    (maphash (lambda (name buffer)
               (push (cons name buffer) sessions))
             org-babel-duckdb-sessions)
    (if (called-interactively-p 'interactive)
        (message "Active sessions: %s"
                 (if sessions
                     (mapconcat #'car (sort sessions (lambda (a b) (string< (car a) (car b)))) ", ")
                   "none"))
      (sort sessions (lambda (a b) (string< (car a) (car b)))))))

;;;###autoload
(defun org-babel-duckdb-create-session (session-name &optional db-file)
  "Create DuckDB session SESSION-NAME, optionally connected to DB-FILE.

If session exists with live process, return existing session.
Returns session buffer.

Uses `org-babel-duckdb-initiate-session' for initialization.
To delete session, use `org-babel-duckdb-delete-session'.
To view sessions, use `org-babel-duckdb-display-sessions'."
  (interactive "sSession name: \nfDatabase file (optional): ")
  (let ((params (if db-file (list (cons :db db-file)) nil)))
    (org-babel-duckdb-initiate-session session-name params)))

;;;###autoload
(defun org-babel-duckdb-delete-session (session-name)
  "Delete DuckDB session SESSION-NAME.

Sends .exit to DuckDB, kills process if needed, kills buffer,
and removes from `org-babel-duckdb-sessions'.

Calls `org-babel-duckdb-cleanup-sessions' after deletion.
To view remaining sessions, use `org-babel-duckdb-display-sessions'."
  (interactive
   (list (completing-read "Delete session: "
                          (mapcar #'car (org-babel-duckdb-list-sessions))
                          nil t)))
  (let ((buffer (gethash session-name org-babel-duckdb-sessions)))
    (when buffer
      (when (buffer-live-p buffer)
        (let ((proc (get-buffer-process buffer)))
          (when (and proc (process-live-p proc))
            (process-send-string proc ".exit\n")
            (sleep-for 0.1)
            (when (process-live-p proc)
              (delete-process proc))))
        (kill-buffer buffer))
      (remhash session-name org-babel-duckdb-sessions)
      (message "Session %s deleted" session-name))))

;;;###autoload
(defun org-babel-duckdb-display-sessions ()
  "Display detailed information about all active DuckDB sessions.

Shows session name, database file, status, and buffer name.
Database file retrieved from buffer-local `org-babel-duckdb-session-db-file'.

Uses `org-babel-duckdb-list-sessions' to enumerate sessions.
For programmatic access, use that function directly."
  (interactive)
  (with-help-window "*DuckDB Sessions*"
    (let ((sessions (org-babel-duckdb-list-sessions)))
      (if (null sessions)
          (princ "No active DuckDB sessions.\n\n")
        (let* ((db-paths (mapcar
                          (lambda (entry)
                            (let* ((buffer (cdr entry))
                                   (db-file (and (buffer-live-p buffer)
                                                 (buffer-local-value 'org-babel-duckdb-session-db-file buffer))))
                              (cond
                               ((null db-file) "in-memory")
                               (t (abbreviate-file-name db-file)))))
                          sessions))
               (max-db-width (max 25 (apply #'max (mapcar #'length db-paths))))
               (name-width 20)
               (status-width 10)
               (total-width (+ name-width max-db-width status-width 4))
               (format-string (concat "%-20s %-" (number-to-string max-db-width) "s %-10s\n")))
          
          (princ "Active DuckDB Sessions:\n\n")
          (princ (format format-string 
                         "SESSION NAME" 
                         "DATABASE" 
                         "STATUS"))
          (princ (make-string total-width ?-))
          (princ "\n")

          (dolist (entry sessions)
            (let* ((name (car entry))
                   (buffer (cdr entry))
                   (proc (and (buffer-live-p buffer) (get-buffer-process buffer)))
                   (status (cond
                            ((not (buffer-live-p buffer)) "BUFFER DEAD")
                            ((not proc) "NO PROCESS")
                            ((process-live-p proc) "ACTIVE")
                            (t "TERMINATED")))
                   (db-file (if (buffer-live-p buffer)
                                (buffer-local-value 'org-babel-duckdb-session-db-file buffer)
                              nil))
                   (db-display (cond
                                ((null db-file) "in-memory")
                                (t (abbreviate-file-name db-file)))))
              (princ (format format-string
                             name
                             db-display
                             status)))))))))

;;;; Execution Cancellation Commands

;;;###autoload
(defun org-babel-duckdb-cancel-execution (exec-id)
  "Cancel async execution EXEC-ID.

Cancellation behavior depends on execution position in queue:
- Currently executing (position 1): Sends SIGINT via `interrupt-process'
- Queued (position 2+): Marks as cancelled, skips when position reached

Updates status via `org-babel-duckdb--update-exec-status'.
Fires `org-babel-duckdb-execution-completed-functions' hook.

Finds execution by searching all active sessions for matching exec-id
in their queues.

EXEC-ID is execution UUID string.

When interactive, provides completion from all queued executions.

To cancel current block, use `org-babel-duckdb-cancel-block-at-point'.
To view running executions, see `org-babel-duckdb-show-queue'."
  (interactive
   (list (let ((queued-ids nil))
           (maphash (lambda (_session queue-info)
                      (dolist (id (plist-get queue-info :pending))
                        (push id queued-ids)))
                    org-babel-duckdb--session-queues)
           (if queued-ids
               (completing-read "Cancel execution: " queued-ids)
             (read-string "Execution ID: ")))))

  (let ((found-session nil)
        (queue-position nil))
    
    ;; Find session and position
    (catch 'found
      (maphash (lambda (session queue-info)
                 (let* ((pending (plist-get queue-info :pending))
                        (pos (cl-position exec-id pending :test #'equal)))
                   (when pos
                     (setq found-session session
                           queue-position pos)
                     (throw 'found t))))
               org-babel-duckdb--session-queues))

    (if found-session
        (cond
         ;; Position 0: Currently executing, interrupt process
         ((zerop queue-position)
          (when-let* ((session-buffer (gethash found-session org-babel-duckdb-sessions))
                      (process (and (buffer-live-p session-buffer)
                                    (get-buffer-process session-buffer)))
                      ((process-live-p process)))
            (interrupt-process process)
            (message "[ob-duckdb] Interrupted execution %s"
                     (if (> (length exec-id) 8)
                         (substring exec-id 0 8)
                       exec-id))
            
            ;; Status update handled by completion-handler from process sentinel
            ;; Don't call here to avoid duplicate hook firing
            ))

         ;; Position 1+: Queued, mark as cancelled without interrupting
         (t
          (org-babel-duckdb--update-exec-status exec-id 'cancelled)
          (run-hook-with-args 'org-babel-duckdb-execution-completed-functions
                              exec-id 'cancelled "User cancelled queued execution")
          (message "[ob-duckdb] Marked queued execution %s as cancelled"
                   (if (> (length exec-id) 8)
                       (substring exec-id 0 8)
                     exec-id))))
      
      ;; Not found in any queue
      (message "[ob-duckdb] Execution %s not found or already completed"
               (if (> (length exec-id) 8)
                   (substring exec-id 0 8)
                 exec-id)))))

;;;###autoload
(defun org-babel-duckdb-cancel-block-at-point ()
  "Cancel async execution for DuckDB block at point.

Extracts exec-id from results placeholder.

Point must be in DuckDB source block with pending async execution.

Delegates to `org-babel-duckdb-cancel-execution' with discovered exec-id.

Also see `org-babel-duckdb--extract-exec-id-from-results' for
placeholder parsing."
  (interactive)
  (let ((exec-id (org-babel-duckdb--extract-exec-id-from-results)))
    (if exec-id
        (org-babel-duckdb-cancel-execution exec-id)
      (message "[ob-duckdb] No active execution found for current block"))))

;;;; Main Execution Entry Point

(defun org-babel-execute:duckdb (body params)
  "Execute DuckDB SQL BODY with PARAMS.

Main entry point called by Org Babel when executing duckdb source blocks.

BODY is SQL code as string.
PARAMS is alist of header arguments (see `org-babel-header-args:duckdb').

Returns output string for sync execution, placeholder for async.

Validates async requirements (must have session).
Expands variables via `org-babel-expand-body:duckdb'.
Generates dot commands via `org-babel-duckdb-process-params'.
Dispatches to `org-babel-duckdb-execute-sync' or
`org-babel-duckdb-execute-async'.

Fires `org-babel-duckdb-execution-started-functions' hook at start.

Block tracking is optional via org-duckdb-blocks.el.
When disabled, only async executions are tracked minimally via
`org-babel-duckdb--pending-async'."
  (message "[ob-duckdb] Starting execution")
  (let* ((session (cdr (assq :session params)))
         (use-async (and (cdr (assq :async params))
                         (string= (cdr (assq :async params)) "yes")))
         (result-params (cdr (assq :result-params params)))
         (exec-id (org-id-uuid))
         (element (org-element-at-point)))

    (when (and use-async
               (or (null session) (string= session "none")))
      (user-error "[ob-duckdb] Asynchronous execution requires a session. Please add ':session name' to your header arguments"))

    (let* ((expanded-body (org-babel-expand-body:duckdb body params))
           (dot-commands (org-babel-duckdb-process-params params))
           (combined-body (if dot-commands
                              (concat dot-commands "\n" expanded-body)
                            expanded-body))
           (final-body combined-body))

      ;; Fire execution started hook
      (run-hook-with-args 'org-babel-duckdb-execution-started-functions
                          exec-id session final-body params use-async element)

      (if use-async
          (progn
            ;; Store block name for queue display
            (when-let ((name (org-element-property :name element)))
              (puthash exec-id name org-babel-duckdb--exec-names))

            ;; Store location for async routing
            (let ((marker (point-marker)))
              (puthash exec-id
                       (cons (current-buffer) marker)
                       org-babel-duckdb--pending-async))

            ;; Start async execution
            (org-babel-duckdb-execute-async session final-body params exec-id)

            ;; Return placeholder text for Org to insert
            (format "DuckDB Execution in Progress\nExecution ID: %s"
                    exec-id))

        ;; Sync execution
        (let ((raw-result (org-babel-duckdb-execute-sync session final-body params)))

          ;; Fire completion hook for sync execution
          (run-hook-with-args 'org-babel-duckdb-execution-completed-functions
                              exec-id 'completed nil)

          ;; Process and return result
          (let* ((cleaned-output (org-babel-duckdb-clean-output raw-result))
                 (output-type (cdr (assq :output params)))
                 (use-buffer-output (and output-type (string= output-type "buffer"))))

            (if use-buffer-output
                (progn
                  (org-babel-duckdb-display-buffer cleaned-output)
                  "Output sent to buffer.")

              (if (member "table" result-params)
                  (org-babel-duckdb-table-or-scalar cleaned-output)
                cleaned-output))))))))

;;;; Language Integration and Initialization

(add-to-list 'org-babel-tangle-lang-exts '("duckdb" . "sql"))
(add-to-list 'org-src-lang-modes '("duckdb" . sql))

(defun org-babel-duckdb-babel-ansi ()
  "Process ANSI color codes in most recent Babel result.

Called automatically after block execution via
`org-babel-after-execute-hook'.

Uses `org-babel-where-is-src-block-result' to locate result region."
  (when-let ((beg (org-babel-where-is-src-block-result nil nil)))
    (save-excursion
      (goto-char beg)
      (when (looking-at org-babel-result-regexp)
        (let ((end (org-babel-result-end))
              (ansi-color-context-region nil))
          (ansi-color-apply-on-region beg end))))))

(add-hook 'org-babel-after-execute-hook 'org-babel-duckdb-babel-ansi)

(provide 'ob-duckdb)

;;; ob-duckdb.el ends here
