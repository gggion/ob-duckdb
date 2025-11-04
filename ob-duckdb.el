;;; ob-duckdb.el --- Org Babel functions for DuckDB SQL -*- lexical-binding: t; -*-

;; Author: gggion
;; Maintainer: gggion <contact@example.com>
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
;;
;; For complete documentation, see Info node `(ob-duckdb) Top' (IN PROGRESS)
;; or visit <https://github.com/gggion/ob-duckdb>
;;
;; Key commands:
;; - `org-babel-duckdb-create-session' - Start a session
;; - `org-babel-duckdb-display-sessions' - Show active sessions
;; - `org-babel-duckdb-cancel-current-execution' - Cancel running query

;;; Code:

(require 'ob)
(require 'ansi-color)
(require 'org-duckdb-blocks)

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
    (async     . :any)
    (output    . :any)
    (max-rows  . :any))
  "DuckDB-specific header arguments for source blocks.
See Info node `(org-babel-duckdb) Header Arguments' for specifications.

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
`org-babel-duckdb-process-and-update-result' for when this is used."
  :type 'string
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

(defcustom org-babel-duckdb-prompt-char "🦆"
  "Character used as DuckDB prompt in session buffers.
Must be unique enough not to appear in query results, as all occurrences
are removed from output by `org-babel-duckdb-clean-output'.

-Examples of good prompt characters:
-- \"🦆\"  (default)
-- \"⬤➤\"
-- \"⬤◗\"
-- \"D>\"

Set by `org-babel-duckdb-initiate-session' when creating sessions."
  :type 'string
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

(defcustom org-babel-duckdb-show-progress t
  "Whether to show status indicators during asynchronous execution.

When non-nil, async queries display a status buffer showing execution
activity via `org-babel-duckdb-create-progress-monitor'.

The buffer auto-closes 3 seconds after completion (see
`org-babel-duckdb-cleanup-progress-display').

Only applies to async execution (see `org-babel-duckdb-execute-async')."
  :type 'boolean
  :group 'org-babel-duckdb
  :package-version '(ob-duckdb . "1.0.0"))

(defcustom org-babel-duckdb-max-rows 200000
  "Maximum number of output lines to display from query results.

Prevents Emacs from freezing when inserting massive result sets.
Set to nil to disable truncation (may cause hangs on large outputs).

Can be overridden per-block with :max-rows header argument.

Truncation is applied by `org-babel-duckdb-read-file-lines' during
result processing. For async execution behavior, see
`org-babel-duckdb-async-sentinel'.

Performance implications:
- Below 50k lines: Negligible overhead
- 50k-200k lines: Noticeable delay (2-5 seconds)
- Above 200k lines: Risk of UI freezing

Also see `org-babel-duckdb-output-buffer' for alternative result display."
  :type '(choice (integer :tag "Maximum rows to display")
                 (const :tag "No limit (may freeze Emacs)" nil))
  :group 'org-babel-duckdb
  :link '(info-link "(org-babel-duckdb) Result Handling")
  :package-version '(ob-duckdb . "1.0.0"))

;;;; Internal Variables

(defvar org-babel-duckdb-sessions (make-hash-table :test 'equal)
  "Hash table of active DuckDB session buffers.
Keys are session names (strings), values are buffer objects.

Managed by `org-babel-duckdb-initiate-session',
`org-babel-duckdb-delete-session', and
`org-babel-duckdb-cleanup-sessions'.

Query with `org-babel-duckdb-list-sessions'.")

(defvar org-babel-duckdb-last-registration nil
  "Most recent block registration info from org-duckdb-blocks.

Plist with :block-id and :exec-id for routing async results.
Set by `org-babel-duckdb-capture-registration-advice', consumed by
`org-babel-execute:duckdb', then reset to nil.

Part of the communication channel between `org-duckdb-blocks-register-execution'
and execution functions.")

(defvar org-babel-duckdb-progress-buffers (make-hash-table :test 'equal)
  "Hash table tracking progress display buffers by execution ID.
Keys are execution IDs (UUID strings), values are buffer objects.

Managed by `org-babel-duckdb-create-progress-monitor' and
`org-babel-duckdb-cleanup-progress-display'.

Only populated when `org-babel-duckdb-show-progress' is non-nil.")

;;;; Session Management Functions

(defun org-babel-duckdb-get-session-buffer (session-name)
  "Get or create buffer for SESSION-NAME.

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

(defun org-babel-duckdb-var-to-duckdb (var)
  "Convert Elisp value VAR to DuckDB SQL literal.

Handles:
- Nested lists as VALUES clauses
- Flat lists as array literals
- 'hline as NULL
- Strings with escaped quotes
- Numbers as-is

Used by `org-babel-expand-body:duckdb' during variable substitution.
Also see `org-babel-variable-assignments:duckdb'."
  (cond
   ((listp var)
    (if (and (equal (length var) 1) (listp (car var)))
        (format "VALUES %s"
                (mapconcat
                 (lambda (row)
                   (concat "(" (mapconcat #'org-babel-duckdb-var-to-duckdb row ", ") ")"))
                 var
                 ", "))
      (concat "[" (mapconcat #'org-babel-duckdb-var-to-duckdb var ", ") "]")))
   ((eq var 'hline) "NULL")
   ((stringp var) (format "'%s'" (replace-regexp-in-string "'" "''" var)))
   ((null var) "NULL")
   (t (format "%s" var))))

(defun org-babel-variable-assignments:duckdb (params)
  "Return list of DuckDB SET statements for variables in PARAMS.

Currently unused - variable substitution happens textually via
`org-babel-expand-body:duckdb'.

Uses `org-babel-duckdb-var-to-duckdb' for value conversion."
  (let ((vars (org-babel--get-vars params)))
    (mapcar
     (lambda (pair)
       (format "%s=%s"
               (car pair)
               (org-babel-duckdb-var-to-duckdb (cdr pair))))
     vars)))

(defun org-babel-expand-body:duckdb (body params)
  "Expand BODY with variables from PARAMS, adding prologue and epilogue.

Variable substitution supports three patterns:
1. varname[key]  - Access table cell by key
2. $varname      - Direct substitution
3. varname       - Word-boundary matched substitution

Also prepends :prologue and appends :epilogue if present in PARAMS.

Uses `org-babel-duckdb-var-to-duckdb' for value conversion.
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

Used by `org-babel-duckdb-execute-sync' and `org-babel-duckdb-async-sentinel'
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
Called by `org-babel-duckdb-process-and-update-result'."
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

(defun org-babel-duckdb-execute-async (session body params block-id exec-id)
  "Execute BODY asynchronously in SESSION without blocking Emacs.

BODY is SQL to execute.
SESSION is session name (required for async, see `org-babel-duckdb-initiate-session').
PARAMS is alist of header arguments.
BLOCK-ID and EXEC-ID are UUIDs from `org-duckdb-blocks-register-execution'.

Returns process object.

Execution uses file-based output redirection for performance with large
results. Completion detected via unique marker in process output.

Results routed back via `org-babel-duckdb-async-sentinel'.
Progress monitoring controlled by `org-babel-duckdb-show-progress'.

To cancel, use `org-babel-duckdb-cancel-execution'.
For synchronous execution, see `org-babel-duckdb-execute-sync'."
  (when-let* ((registry-info (gethash block-id org-duckdb-blocks-registry))
              (file (plist-get registry-info :file))
              (buffer-name (plist-get registry-info :buffer))
              (begin (plist-get registry-info :begin))
              (buf (or (and file (file-exists-p file) (find-file-noselect file))
                       (get-buffer buffer-name))))

    (with-current-buffer buf
      (save-excursion
        (goto-char begin)
        (org-babel-remove-result)
        (org-babel-insert-result "Executing asynchronously..."
                                 (cdr (assq :result-params params))))))

  (let* ((session-buffer (org-babel-duckdb-initiate-session session params))
         (process (get-buffer-process session-buffer))
         (temp-out-file (org-babel-temp-file "duckdb-async-out-"))
         (temp-err-file (org-babel-temp-file "duckdb-async-err-"))
         (temp-script-file (org-babel-duckdb-write-temp-sql body))
         (result-params (cdr (assq :result-params params)))
         (completion-marker (format "ASYNC_COMPLETE_%s" exec-id))
         (original-filter (process-filter process)))

    (org-duckdb-blocks-update-execution-status exec-id 'running)
    (org-duckdb-blocks-store-process exec-id process)

    (when org-babel-duckdb-show-progress
      (org-babel-duckdb-create-progress-monitor exec-id session-buffer))

    (let ((execution-sequence
           (concat
            ".bail on\n"
            (format ".output %s\n" (expand-file-name temp-out-file))
            (format ".read %s\n" (expand-file-name temp-script-file))
            ".output\n"
            (format ".print \"%s\"\n" completion-marker))))

      (set-process-filter
       process
       (lambda (proc string)
         (when (gethash exec-id org-babel-duckdb-progress-buffers)
           (org-babel-duckdb-update-progress-display exec-id string))

         (when (string-match-p "\\(Error:\\|Exception:\\|SYNTAX_ERROR\\|CATALOG_ERROR\\)" string)
           (org-babel-duckdb-capture-session-error exec-id string temp-err-file))

         (when (string-search completion-marker string)
           (set-process-filter proc original-filter)
           (funcall (org-babel-duckdb-async-sentinel
                     exec-id temp-out-file temp-err-file temp-script-file
                     block-id params result-params)
                    proc "finished\n"))

         (when original-filter
           (funcall original-filter proc string))))

      (process-send-string process execution-sequence)
      process)))

(defun org-babel-duckdb-async-sentinel (exec-id temp-out-file temp-err-file temp-script-file
                                               block-id params result-params)
  "Create sentinel for async execution EXEC-ID.

Returns function called when execution completes.
Reads output from TEMP-OUT-FILE, classifies status, updates BLOCK-ID with
results, and cleans up temporary files.

TEMP-ERR-FILE and TEMP-SCRIPT-FILE are also cleaned up.
PARAMS and RESULT-PARAMS control result insertion (see
`org-babel-duckdb-process-and-update-result').

Status tracking via `org-duckdb-blocks-update-execution-status'.
Progress cleanup via `org-babel-duckdb-cleanup-progress-display'.
Truncation via `org-babel-duckdb-get-max-rows' and
`org-babel-duckdb-read-file-lines'.

The sentinel is installed by `org-babel-duckdb-execute-async'."
  (lambda (process event)
    (let ((exit-status (process-exit-status process))
          (status-msg (string-trim event))
          (max-rows (org-babel-duckdb-get-max-rows params)))

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

                (cond
                 ((and (string-match-p "finished" event) (zerop exit-status))
                  (cond
                   (has-errors
                    (org-duckdb-blocks-update-execution-status
                     exec-id 'error (format "DuckDB Error: %s" stderr-content))
                    (org-babel-duckdb-process-and-update-result
                     block-id (format "DuckDB Error:\n%s" stderr-content)
                     params result-params))
                   (has-output
                    (org-duckdb-blocks-update-execution-status exec-id 'completed)
                    (org-babel-duckdb-process-and-update-result
                     block-id stdout-content params result-params))
                   (t
                    (org-duckdb-blocks-update-execution-status
                     exec-id 'warning "No output produced")
                    (org-babel-duckdb-process-and-update-result
                     block-id "Query completed but produced no output"
                     params result-params))))

                 ((string-match-p "\\(killed\\|terminated\\|interrupt\\)" event)
                  (org-duckdb-blocks-update-execution-status exec-id 'cancelled status-msg)
                  (org-babel-duckdb-process-and-update-result
                   block-id (format "Execution cancelled: %s" status-msg)
                   params result-params))

                 ((not (zerop exit-status))
                  (let ((error-msg (if has-errors stderr-content
                                    (format "Process exited with code %d" exit-status))))
                    (org-duckdb-blocks-update-execution-status exec-id 'error error-msg)
                    (org-babel-duckdb-process-and-update-result
                     block-id (format "DuckDB Error (exit code %d):\n%s"
                                      exit-status error-msg)
                     params result-params)))

                 (t
                  (org-duckdb-blocks-update-execution-status exec-id 'unknown event)
                  (org-babel-duckdb-process-and-update-result
                   block-id (format "Unknown process status: %s" event)
                   params result-params))))

            (error
             (let ((error-msg (format "Sentinel error: %S" err)))
               (org-duckdb-blocks-update-execution-status exec-id 'error error-msg)
               (org-babel-duckdb-process-and-update-result
                block-id error-msg params result-params))))

        (org-babel-duckdb-cleanup-progress-display exec-id)
        (when (file-exists-p temp-out-file) (delete-file temp-out-file))
        (when (file-exists-p temp-err-file) (delete-file temp-err-file))
        (when (file-exists-p temp-script-file) (delete-file temp-script-file))))))

(defun org-babel-duckdb-capture-session-error (exec-id error-output temp-err-file)
  "Capture ERROR-OUTPUT for EXEC-ID by appending to TEMP-ERR-FILE.

Adds timestamp to each error for correlation with progress displays (see
`org-babel-duckdb-update-progress-display').

Called by async execution filter when errors detected.
Errors read by `org-babel-duckdb-async-sentinel'."
  (with-temp-file temp-err-file
    (when (file-exists-p temp-err-file)
      (insert-file-contents temp-err-file))
    (goto-char (point-max))
    (insert (format "[%s] %s\n"
                    (format-time-string "%H:%M:%S")
                    (string-trim error-output)))))

;;;;; Result Processing

(defun org-babel-duckdb-process-and-update-result (block-id raw-output params result-params)
  "Process RAW-OUTPUT and update results for BLOCK-ID.

Cleans output via `org-babel-duckdb-clean-output', determines display
location (inline vs buffer per :output in PARAMS), and inserts into
appropriate location with ANSI color processing.

RESULT-PARAMS control formatting and insertion (see `org-babel-insert-result').

Block location resolved via BLOCK-ID in `org-duckdb-blocks-registry'.

For buffered output, uses `org-babel-duckdb-display-buffer'.
For table results, uses `org-babel-duckdb-table-or-scalar'.

Called by both `org-babel-duckdb-execute-sync' and
`org-babel-duckdb-async-sentinel'."
  (message "[ob-duckdb] Processing results for block %s" (substring block-id 0 8))

  (let* ((cleaned-output (org-babel-duckdb-clean-output raw-output))
         (output-type (cdr (assq :output params)))
         (use-buffer-output (and output-type (string= output-type "buffer"))))

    (when-let* ((registry-info (gethash block-id org-duckdb-blocks-registry))
                (file (plist-get registry-info :file))
                (buffer-name (plist-get registry-info :buffer))
                (begin (plist-get registry-info :begin))
                (buf (or (and file (file-exists-p file) (find-file-noselect file))
                         (get-buffer buffer-name))))

      (with-current-buffer buf
        (save-excursion
          (goto-char begin)
          (let ((element (org-element-at-point)))
            (when (eq (org-element-type element) 'src-block)
              (let* ((header-params (org-element-property :parameters element))
                     (header-args (org-babel-parse-header-arguments header-params))
                     (default-wrap (cdr (assq :wrap org-babel-default-header-args:duckdb)))
                     (header-wrap (cdr (assq :wrap header-args)))
                     (wrap (or header-wrap default-wrap))
                     (effective-params result-params))

                (if use-buffer-output
                    (progn
                      (org-babel-remove-result)
                      (org-babel-insert-result "Output sent to buffer." effective-params)
                      (org-babel-duckdb-display-buffer cleaned-output))

                  (progn
                    (when (and wrap (not (assq :wrap effective-params)))
                      (setq effective-params
                            (cons (cons :wrap wrap) effective-params)))

                    (when (and (stringp cleaned-output) (string-match-p "\e\\[" cleaned-output))
                      (with-temp-buffer
                        (insert cleaned-output)
                        (ansi-color-apply-on-region (point-min) (point-max))
                        (setq cleaned-output (buffer-string))))

                    (condition-case err
                        (progn
                          (org-babel-remove-result)
                          (org-babel-insert-result
                           (if (member "table" result-params)
                               (org-babel-duckdb-table-or-scalar cleaned-output)
                             cleaned-output)
                           effective-params)

                          (when-let ((result-pos (org-babel-where-is-src-block-result)))
                            (save-excursion
                              (goto-char result-pos)
                              (when (looking-at org-babel-result-regexp)
                                (let ((end (org-babel-result-end))
                                      (ansi-color-context-region nil))
                                  (ansi-color-apply-on-region result-pos end))))))
                      (error
                       (message "[ob-duckdb] Error updating result: %S" err)))))))))))

    cleaned-output))

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

Called by `org-babel-duckdb-process-and-update-result' when :output buffer
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
  "Create status monitoring buffer for async execution EXEC-ID.

Displays timestamped messages in side window.
SESSION-BUFFER currently unused.

Registers buffer in `org-babel-duckdb-progress-buffers'.
Updates via `org-babel-duckdb-update-progress-display'.
Cleanup via `org-babel-duckdb-cleanup-progress-display'.

Only called when `org-babel-duckdb-show-progress' is non-nil."
  (let* ((progress-buffer-name (format "*DuckDB Progress-%s*" (substring exec-id 0 8)))
         (progress-buffer (get-buffer-create progress-buffer-name)))

    (puthash exec-id progress-buffer org-babel-duckdb-progress-buffers)

    (with-current-buffer progress-buffer
      (erase-buffer)
      (insert (format "DuckDB Execution Progress\nExecution ID: %s\n\n" exec-id))
      (goto-char (point-max)))

    (display-buffer progress-buffer
                   '(display-buffer-in-side-window
                     (side . bottom)
                     (window-height . 0.15)))

    progress-buffer))

(defun org-babel-duckdb-update-progress-display (exec-id output)
  "Update status display for EXEC-ID with new OUTPUT from process.

Filters OUTPUT for status lines and appends with timestamps.

Called by async execution filter installed by
`org-babel-duckdb-execute-async'.

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

Appends completion message and schedules buffer kill after 3 seconds.

Removes EXEC-ID from `org-babel-duckdb-progress-buffers'.

Called by `org-babel-duckdb-async-sentinel'."
  (when-let ((progress-buffer (gethash exec-id org-babel-duckdb-progress-buffers)))
    (when (buffer-live-p progress-buffer)
      (with-current-buffer progress-buffer
        (goto-char (point-max))
        (insert (format "\n[%s] Execution completed.\n"
                        (format-time-string "%H:%M:%S"))))
      (run-with-timer 3.0 nil
                     (lambda ()
                       (when (buffer-live-p progress-buffer)
                         (kill-buffer progress-buffer)))))
    (remhash exec-id org-babel-duckdb-progress-buffers)))

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

Uses `org-babel-duckdb-list-sessions' to enumerate sessions.
For programmatic access, use that function directly."
  (interactive)
  (with-help-window "*DuckDB Sessions*"
    (let ((sessions (org-babel-duckdb-list-sessions)))
      (if (null sessions)
          (princ "No active DuckDB sessions.\n\n")
        (princ "Active DuckDB Sessions:\n\n")
        (princ (format "%-20s %-25s %-10s\n" "SESSION NAME" "DATABASE" "STATUS"))
        (princ (make-string 60 ?-))
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
                 (db-file "N/A"))
            (princ (format "%-20s %-25s %-10s\n"
                           name
                           db-file
                           status))))))))

;;;; Execution Cancellation Commands

;;;###autoload
(defun org-babel-duckdb-cancel-execution (exec-id)
  "Cancel async execution EXEC-ID.

Sends SIGINT to process via `interrupt-process'.
Updates status via `org-duckdb-blocks-update-execution-status'.

To cancel current block, use `org-babel-duckdb-cancel-current-execution'.
To view running executions, see `org-duckdb-blocks-show-execution-status'."
  (interactive
   (list (completing-read "Cancel execution: "
                          (org-duckdb-blocks-get-running-executions))))
  (if-let ((process (org-duckdb-blocks-get-process exec-id)))
      (when (process-live-p process)
        (interrupt-process process)
        (message "[ob-duckdb] Interrupted execution %s" (substring exec-id 0 8))
        (org-duckdb-blocks-update-execution-status exec-id 'cancelled))
    (message "[ob-duckdb] Execution %s not found or already completed"
            (substring exec-id 0 8))))

;;;###autoload
(defun org-babel-duckdb-cancel-current-execution ()
  "Cancel most recent async execution in DuckDB block at point.

Point must be in a DuckDB source block.

Uses `org-duckdb-blocks-get-latest-exec-id-for-block' to find execution.
Delegates to `org-babel-duckdb-cancel-execution'."
  (interactive)
  (if-let* ((element (org-element-at-point))
            (is-duckdb (and (eq (car element) 'src-block)
                           (string= (org-element-property :language element) "duckdb")))
            (begin (org-element-property :begin element))
            (exec-id (org-duckdb-blocks-get-latest-exec-id-for-block begin)))
      (org-babel-duckdb-cancel-execution exec-id)
    (message "[ob-duckdb] No active execution found for current block")))

;;;; Debug Utilities

(defun org-babel-duckdb-debug-show-process-buffer (session-name)
  "Show comint process buffer for SESSION-NAME.

Displays raw DuckDB interaction for debugging.

Buffer managed by `org-babel-duckdb-initiate-session'."
  (interactive "sSession name: ")
  (let ((buffer (gethash session-name org-babel-duckdb-sessions)))
    (if buffer
        (display-buffer buffer)
      (message "No session buffer found for %s" session-name))))

(defun org-babel-duckdb-debug-recent-outputs ()
  "Display recent async execution output buffers.

Shows contents of temporary output buffers for debugging.

Searches for buffers created by `org-babel-duckdb-execute-async'."
  (interactive)
  (with-output-to-temp-buffer "*DuckDB Async Debug*"
    (let ((buffers (buffer-list)))
      (dolist (buf buffers)
        (when (string-match-p "\\*duckdb-async-output-" (buffer-name buf))
          (princ (format "=== Buffer: %s ===\n" (buffer-name buf)))
          (with-current-buffer buf
            (princ (buffer-string)))
          (princ "\n\n"))))))

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

Requires `org-duckdb-blocks-setup' for block tracking.

See Info node `(org-babel-duckdb) Execution' for details."
  (message "[ob-duckdb] Starting execution")

  (unless (advice-member-p 'org-duckdb-blocks-register-execution 'org-babel-execute-src-block)
    (org-duckdb-blocks-setup))

  (let* ((session (cdr (assq :session params)))
         (use-async (and (cdr (assq :async params))
                         (string= (cdr (assq :async params)) "yes")))
         (result-params (cdr (assq :result-params params))))

    (when (and use-async
               (or (null session) (string= session "none")))
      (user-error "[ob-duckdb] Asynchronous execution requires a session. Please add ':session name' to your header arguments"))

    (let* ((expanded-body (org-babel-expand-body:duckdb body params))
           (dot-commands (org-babel-duckdb-process-params params))
           (combined-body (if dot-commands
                              (concat dot-commands "\n" expanded-body)
                            expanded-body))
           (final-body combined-body))

      (if use-async
          (progn
            (unless org-babel-duckdb-last-registration
              (error "No block registration info available"))

            (let ((block-id (plist-get org-babel-duckdb-last-registration :block-id))
                  (exec-id (plist-get org-babel-duckdb-last-registration :exec-id)))

              (setq org-babel-duckdb-last-registration nil)

              (org-babel-duckdb-execute-async session final-body params block-id exec-id)
              "Executing asynchronously..."))

        (let* ((raw-result (org-babel-duckdb-execute-sync session final-body params))
               (block-id nil)
               (exec-id nil))

          (when org-babel-duckdb-last-registration
            (setq block-id (plist-get org-babel-duckdb-last-registration :block-id)
                  exec-id (plist-get org-babel-duckdb-last-registration :exec-id))
            (setq org-babel-duckdb-last-registration nil))

          (if (and block-id exec-id)
              (org-babel-duckdb-process-and-update-result block-id raw-result params result-params)

            (let* ((cleaned-output (org-babel-duckdb-clean-output raw-result))
                   (output-type (cdr (assq :output params)))
                   (use-buffer-output (and output-type (string= output-type "buffer"))))

              (if use-buffer-output
                  (progn
                    (org-babel-duckdb-display-buffer cleaned-output)
                    "Output sent to buffer.")

                (if (member "table" result-params)
                    (org-babel-duckdb-table-or-scalar cleaned-output)
                  cleaned-output)))))))))

;;;; Registration Capture Advice

(defun org-babel-duckdb-capture-registration-advice (result)
  "Capture block registration info from RESULT for execution routing.

Stores plist with :block-id and :exec-id in
`org-babel-duckdb-last-registration'.

Installed as :filter-return advice on
`org-duckdb-blocks-register-execution'.

RESULT is plist from registration function."
  (when result
    (setq org-babel-duckdb-last-registration result))
  result)

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

(advice-add 'org-duckdb-blocks-register-execution :filter-return
            #'org-babel-duckdb-capture-registration-advice)

(add-hook 'org-babel-after-execute-hook 'org-babel-duckdb-babel-ansi)

(org-duckdb-blocks-setup)

(provide 'ob-duckdb)

;;; ob-duckdb.el ends here
