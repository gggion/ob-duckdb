;;; ob-duckdb.el --- Org Babel functions for DuckDB SQL -*- lexical-binding: t; -*-

;; Author: gggion
;; Maintainer: gggion
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.4"))
;; Keywords: languages, org, babel, duckdb, sql
;; URL: https://github.com/gggion/ob-duckdb.el

;;; Commentary:
;;
;; This package provides comprehensive Org Babel integration for DuckDB, an
;; in-process analytical SQL database. It enables executing DuckDB queries
;; directly in Org mode source blocks with robust support for DuckDB's
;; full feature set.
;;
;; DuckDB (https://duckdb.org/) is a high-performance analytical database
;; designed for in-process analytics that combines exceptional analytical
;; performance with a friendly SQL interface. This package brings DuckDB's
;; analytical capabilities directly into Org documents, enabling data
;; exploration, visualization, and documentation in a single environment.
;;
;; Key features include:
;;
;; - Execute DuckDB SQL queries directly in Org source blocks
;; - Format query results in various styles (table, box, markdown, JSON, etc.)
;; - Maintain persistent database connections through sessions
;; - Connect to database files or use in-memory databases
;; - Asynchronous query execution with :async yes header
;; - Variable substitution from Org mode elements into SQL queries
;; - Full support for DuckDB dot commands (.mode, .print, .shell, etc.)
;; - Custom header arguments for controlling query execution and formatting
;;
;; Basic usage example:
;;
;; #+begin_src duckdb
;;   SELECT * FROM generate_series(1, 5) AS s(num);
;; #+end_src
;;
;; Example with database connection and formatting:
;;
;; #+begin_src duckdb :db mydata.duckdb :format markdown :headers on
;;   SELECT * FROM mytable LIMIT 10;
;; #+end_src
;;
;; For asynchronous execution:
;;
;; #+begin_src duckdb :session mydb :async yes
;;   -- This runs in the background without blocking Emacs
;;   SELECT * FROM large_table WHERE complex_condition;
;; #+end_src

;;; Code:

(require 'org-macs)
(require 'ob)
(require 'ob-ref)
(require 'ob-comint)
(require 'ob-eval)
(require 'ansi-color)
(require 'org-id)
(require 'org-duckdb-blocks) ;; Block tracking system

;;; Language Registration

;; Define the file extension for tangling duckdb blocks
(add-to-list 'org-babel-tangle-lang-exts '("duckdb" . "sql"))

;; Default header arguments for duckdb blocks
(defvar org-babel-default-header-args:duckdb
  '((:results . "output")
    (:wrap))
  "Default header arguments for duckdb code blocks.
By default, uses `output' with `:wrap' to preserve formatting.
This ensures DuckDB's formatted output is properly captured and
displayed with its original structure and any colorization intact.")

(defconst org-babel-header-args:duckdb
  '((db        . :any)  ; Database file to use
    (format    . :any)  ; Output format (csv, table, json, etc.)
    (timer     . :any)  ; Show execution time
    (headers   . :any)  ; Show column headers
    (nullvalue . :any)  ; String to use for NULL values
    (separator . :any)  ; Column separator
    (echo      . :any)  ; Echo commands
    (bail      . :any)  ; Exit on error
    (async     . :any)  ; Execute asynchronously
    (md_token  . :any)  ; Pass in motherduck_token
    (output    . :any)) ; Output handling (e.g., "buffer")
  "DuckDB-specific header arguments.
These header arguments control how DuckDB executes queries and formats results:

db:        Path to database file
format:    Output format (ascii, box, column, csv, json, markdown, table, etc.)
timer:     Show execution time (on/off)
headers:   Show column headers (on/off)
nullvalue: String to use for NULL values
separator: Column separator for output
echo:      Echo commands being executed (on/off)
bail:      Exit on error (on/off)
async:     Execute asynchronously (yes/no)
md_token:  Motherduck token
output:    Control result display (\"buffer\" for dedicated output)")

;;; Customization Options

(defcustom org-babel-duckdb-command "duckdb"
  "Command used to execute DuckDB.
This should be the path to the DuckDB executable or simply \"duckdb\"
if it's in your PATH. You can also include command-line arguments
that should be used for all DuckDB executions.

The command is used as the basis for both direct command-line invocation
and for establishing interactive sessions. Any arguments included here
will be applied to all DuckDB processes created by this package."
  :type 'string
  :group 'org-babel)

(defcustom org-babel-duckdb-output-buffer "*DuckDB-output*"
  "Buffer name for displaying DuckDB query results.
This buffer is used when the :output header argument is set to \"buffer\".
Results will be displayed in this buffer with ANSI color processing applied.

The buffer provides a non-intrusive way to view large result sets
without cluttering the Org document, and is particularly useful
for exploring data or debugging queries."
  :type 'string
  :group 'org-babel)

(defcustom org-babel-duckdb-prompt-char "🦆"
  "Character or string used as the DuckDB prompt in org-babel sessions.
This prompt is used to identify the DuckDB prompt in the output buffer.
It should be a unique character or string that won't appear in your
actual query results, as ALL instances of this character will be
removed from the final output.

Examples of good prompt characters:
- \"🦆\"  (default)
- \"⬤➤\"
- \"⬤◗\"
- \"D>\"

CAUTION: Avoid using characters or symbols that might appear in your data
or query results to prevent unintended text removal."
  :type 'string
  :group 'org-babel)

;;; Session Management

(defvar org-babel-duckdb-sessions (make-hash-table :test 'equal)
  "Hash table of active DuckDB session buffers.
Keys are session names and values are the corresponding buffer objects.
This allows multiple independent DuckDB sessions to be managed simultaneously.

Sessions provide persistent database connections throughout an Emacs
session, maintaining state between query executions. Each session
has its own namespace for variables, temporary tables, and settings.")

(defvar org-babel-duckdb-last-registration nil
  "Stores the most recent block registration info from org-duckdb-blocks.
This is a critical communication bridge between the registration phase
and the execution phase. During registration, this variable captures
the block ID and execution ID that uniquely identify the current block
and execution context. These IDs are then used to properly route
results back to the originating block.

The variable is set by advice on `org-duckdb-blocks-register-execution'
and consumed during execution.")

(defun org-babel-duckdb-cleanup-sessions ()
  "Clean up dead DuckDB sessions from the session registry.
This removes sessions whose processes or buffers no longer exist.

The function iterates through all registered sessions, checking if
each session's buffer and process are still alive. If either has
been killed or terminated, the session is removed from the registry.

This helps prevent resource leaks and maintain an accurate session list,
particularly after crashes or unexpected terminations."
  (interactive)
  (maphash
   (lambda (name buffer)
     (when (or (not (buffer-live-p buffer))
               (not (process-live-p (get-buffer-process buffer))))
       (remhash name org-babel-duckdb-sessions)))
   org-babel-duckdb-sessions))

(defun org-babel-duckdb-get-session-buffer (session-name)
  "Get or create a buffer for the DuckDB SESSION-NAME.
If a buffer for the named session already exists, return it.
Otherwise create a new buffer and register it in `org-babel-duckdb-sessions'.
Returns a buffer object dedicated to the named session.

The session system allows maintaining state between queries, including:
- Database connections
- Temporary tables
- User-defined functions
- Session settings and variables

Sessions are created on demand and persist until explicitly terminated
or until Emacs exits. Multiple sessions can exist simultaneously,
each with its own independent state and connection."
  (let ((buffer-name (format "*DuckDB:%s*" session-name)))
    (or (gethash session-name org-babel-duckdb-sessions)
        (let ((new-buffer (get-buffer-create buffer-name)))
          (puthash session-name new-buffer org-babel-duckdb-sessions)
          new-buffer))))

;;; Variable Handling

(defun org-babel-duckdb-var-to-duckdb (var)
  "Convert an Emacs Lisp value VAR to a DuckDB SQL value.
Handles various data types:
- Lists are converted to DuckDB arrays or VALUES expressions
- Org table horizontal separators ('hline) become NULL
- Strings are properly quoted with single quotes
- nil values become NULL
- Other values are converted to their string representation

This function is the core of variable substitution in SQL queries,
allowing Org Babel variables to be seamlessly integrated into
DuckDB statements with proper type conversion and SQL syntax."
  (cond
   ;; Handle tables/lists
   ((listp var)
    (if (and (equal (length var) 1) (listp (car var)))
        ;; This is a table with just data
        (format "VALUES %s"
                (mapconcat
                 (lambda (row)
                   (concat "(" (mapconcat #'org-babel-duckdb-var-to-duckdb row ", ") ")"))
                 var
                 ", "))
      ;; This is a list/array
      (concat "[" (mapconcat #'org-babel-duckdb-var-to-duckdb var ", ") "]")))

   ;; Handle org table horizontal separators
   ((eq var 'hline)
    "NULL")

   ;; Handle strings (escape single quotes by doubling them)
   ((stringp var)
    (format "'%s'" (replace-regexp-in-string "'" "''" var)))

   ;; Handle nil values
   ((null var)
    "NULL")

   ;; Other values (numbers, etc.) - convert to string
   (t
    (format "%s" var))))

(defun org-babel-variable-assignments:duckdb (params)
  "Return list of DuckDB statements assigning variables in PARAMS.
Each variable is converted to its DuckDB equivalent using
`org-babel-duckdb-var-to-duckdb'. This is used when variables
need to be explicitly assigned in the DuckDB session.

The resulting assignments follow DuckDB's variable syntax:
  varname=value

These assignments can be used directly in DuckDB statements or
passed as part of session initialization to make variables
available throughout a session's lifetime."
  (let ((vars (org-babel--get-vars params)))
    (mapcar
     (lambda (pair)
       (format "%s=%s"
               (car pair)
               (org-babel-duckdb-var-to-duckdb (cdr pair))))
     vars)))

(defun org-babel-duckdb-insert-org-table-markers (body)
  "Insert markers around org-table mode sections in BODY.
This processes all `.mode org-table` directives, replacing them with
`.mode markdown` and adding special marker strings around the section.

The transformation creates a virtual format that doesn't exist in DuckDB
itself but provides seamless integration with Org mode tables. The output
is later post-processed to transform marked sections into proper Org tables.

The function identifies all occurrences of `.mode org-table` directives and:
1. Inserts an \"ORG_TABLE_FORMAT_START\" marker before the section
2. Changes the directive to use Markdown format internally
3. Tracks mode changes to properly terminate table sections
4. Inserts an \"ORG_TABLE_FORMAT_END\" marker when section ends

Returns the modified body string with all directives and markers in place."
  ;; Quick check if processing is needed at all
  (if (not (string-match-p "\\.mode\\s-+org-table" body))
      body ; No org-table directives, return unchanged

    (let ((lines (split-string body "\n"))
          (result-lines nil)
          (has-org-table nil))

      ;; Process each line and collect transformed lines
      (dolist (line lines)
        (cond
         ;; Found .mode org-table
         ((string-match-p "^\\s-*\\.mode\\s-+org-table\\s-*$" line)
          (setq has-org-table t)
          (push ".print \"ORG_TABLE_FORMAT_START\"" result-lines)
          (push ".mode markdown" result-lines))

         ;; Found different .mode directive after we've seen org-table
         ((and has-org-table
               (string-match-p "^\\s-*\\.mode\\s-+" line)
               (not (string-match-p "^\\s-*\\.mode\\s-+org-table\\s-*$" line)))
          (push ".print \"ORG_TABLE_FORMAT_END\"" result-lines)
          (push line result-lines)
          (setq has-org-table nil))

         ;; Any other line
         (t
          (push line result-lines))))

      ;; Add trailing end marker if needed
      (when has-org-table
        (push ".print \"ORG_TABLE_FORMAT_END\"" result-lines))

      ;; Join all lines with a single operation
      (mapconcat #'identity (nreverse result-lines) "\n"))))

(defun org-babel-expand-body:duckdb (body params)
  "Expand BODY with variables from PARAMS.
This performs three types of variable substitution:
1. Table cell access with syntax: varname[key]
2. Variable references with syntax: $varname
3. Direct variable name matches: varname

BODY is the source code from the source block.
PARAMS includes the header arguments from the source block.

This advanced variable handling allows flexible use of variables
from other Org elements in DuckDB queries."
  (let ((prologue (cdr (assq :prologue params)))
        (epilogue (cdr (assq :epilogue params))))
    (with-temp-buffer
      (insert body)
      ;; Replace variables directly in the body
      (let ((vars (org-babel--get-vars params)))
        (dolist (pair vars)
          (let ((name (car pair))
                (value (cdr pair)))

            ;; Handle the three substitution patterns in a single pass through the buffer
            (goto-char (point-min))
            (while (re-search-forward
                    (format "\\(?:\\$%s\\|\\b%s\\[\\([^]]+\\)\\]\\|\\b%s\\b\\)"
                            (regexp-quote (symbol-name name))
                            (regexp-quote (symbol-name name))
                            (regexp-quote (symbol-name name)))
                    nil t)
              ;; Check which pattern matched
              (cond
               ;; varname[key] pattern
               ((match-beginning 1)
                (let* ((key (match-string 1))
                       (cell-value nil))
                  ;; Find the value in the table
                  (when (and (listp value) (> (length value) 0))
                    (dolist (row value)
                      (when (and (listp row) (>= (length row) 2)
                                 (equal (car row) key))
                        (setq cell-value (cadr row)))))
                  ;; Replace with the cell value
                  (when cell-value
                    (replace-match (if (stringp cell-value)
                                       cell-value
                                     (format "%S" cell-value))
                                   t t))))

               ;; $varname pattern (match starts with $)
               ((eq (char-before (match-beginning 0)) ?$)
                (replace-match (if (stringp value)
                                   value
                                 (format "%S" value))
                               t t))

               ;; bare varname pattern
               (t
                ;; Only replace if it's not part of vars[something]
                (unless (looking-back "\\[" 1)
                  (replace-match (if (stringp value)
                                     value
                                   (format "%S" value))
                                 t t))))))))

      (setq body (buffer-string)))

    ;; Combine with prologue and epilogue
    (mapconcat #'identity
               (delq nil (list prologue body epilogue))
               "\n")))

;;; Command Processing

(defun org-babel-duckdb-process-params (params)
  "Process header PARAMS and generate appropriate DuckDB dot commands.
Converts header arguments to their corresponding DuckDB configuration commands:
- :format → .mode (output format)
- :timer → .timer (execution timing)
- :headers → .headers (column headers display)
- :nullvalue → .nullvalue (NULL representation)
- :separator → .separator (column delimiter)
- :echo → .echo (command echo)
- :bail → .bail (error handling)
- :md_token → (setenv motherduck_token md_token)

Returns a string of newline-separated dot commands to configure DuckDB.

This function provides the critical link between Babel header arguments
and DuckDB's command structure, ensuring that formatting and behavior
directives are properly translated to DuckDB's native configuration system."
  (let ((format (cdr (assq :format params)))
        (timer (cdr (assq :timer params)))
        (headers (cdr (assq :headers params)))
        (nullvalue (cdr (assq :nullvalue params)))
        (separator (cdr (assq :separator params)))
        (echo (cdr (assq :echo params)))
        (bail (cdr (assq :bail params)))
        (md_token (cdr (assq :md_token params))))

    (when md_token  (setenv "motherduck_token" md_token))

    ;; Use with-temp-buffer for string building which benchmarks showed was fastest
    (with-temp-buffer
      ;; Add each command if its parameter is specified
      (insert (format ".prompt %s\n" org-babel-duckdb-prompt-char))
      (when format    (insert (format ".mode %s\n"      format)))
      (when nullvalue (insert (format ".nullvalue %s\n" nullvalue)))
      (when separator (insert (format ".separator %s\n" separator)))
      (when timer     (insert (format ".timer %s\n"     (if (string= timer   "off") "off" "on"))))
      (when headers   (insert (format ".headers %s\n"   (if (string= headers "off") "off" "on"))))
      (when echo      (insert (format ".echo %s\n"      (if (string= echo    "off") "off" "on"))))
      (when bail      (insert (format ".bail %s\n"      (if (string= bail    "off") "off" "on"))))

      ;; Return the buffer contents if we added any commands
      (when (> (buffer-size) 0)
        (buffer-string)))))

(defun org-babel-duckdb-write-temp-sql (body)
  "Write SQL BODY to a temporary file and return the filename.
Creates a temporary file with DuckDB SQL content that can be executed
via the command line. The filename has a prefix of \"duckdb-\" and
uses `org-babel-temp-file' to ensure proper file management.

Using temporary files provides several benefits:
1. Handles queries of any length without command-line limitations
2. Preserves formatting and whitespace in complex queries
3. Allows DuckDB to process the query as a script with proper parsing
4. Supports multiline statements and comments

The temporary file is managed by Org's temporary file system and will
be cleaned up automatically according to Org's file handling rules."
  (let ((temp-file (org-babel-temp-file "duckdb-")))
    (with-temp-file temp-file
      (insert body))
    temp-file))

(defun org-babel-duckdb-clean-output (output)
  "Clean DuckDB output by removing all prompt characters and other artifacts.
Processes raw DuckDB OUTPUT to remove:
- All instances of `org-babel-duckdb-prompt-char`
- Marker lines and status messages
- Process termination messages
- Excess whitespace

This function normalizes DuckDB's raw terminal output into a form
suitable for inclusion in Org documents. It handles both interactive
session output and command-line execution results, removing various
artifacts that would otherwise clutter the displayed results."
  (let* ((prompt-char (regexp-quote org-babel-duckdb-prompt-char))
         ;; Pass 1: Remove marker lines as a group
         (cleaned-output
          (replace-regexp-in-string
           (rx bol
               (or "DUCKDB_START_" "DUCKDB_END_" "DUCKDB_QUERY_COMPLETE"
                   "ORG_TABLE_FORMAT_START" "ORG_TABLE_FORMAT_END"
                   "Process duckdb finished")
               (0+ not-newline)
               eol)
           "" output))

         ;; Pass 2: Handle file instructions and config lines
         (cleaned-output
          (replace-regexp-in-string
           (rx (or (seq bol "Use \".open FILENAME\"" (0+ not-newline) "\n")
                   (seq bol (0+ space)
                        (or "echo:" "headers:" "mode:")
                        (0+ not-newline) "\n")))
           "" cleaned-output)))

    ;; Pass 3: Remove prompt characters
    (setq cleaned-output (replace-regexp-in-string prompt-char "" cleaned-output))

    ;; Pass 4: Handle progress bars
    (setq cleaned-output (replace-regexp-in-string
                         (rx bol (0+ any) (1+ digit) "% ▕" (0+ any) "▏" (0+ any) eol)
                         ""
                         cleaned-output))

    ;; Pass 5: Clean up whitespace (grouped as one pass)
    (setq cleaned-output
          (replace-regexp-in-string
           (rx (or (seq bol (+ "\n"))          ; Leading newlines
                   (seq (+ "\n") eol)          ; Trailing newlines
                   (seq "\n" (+ "\n"))))       ; Multiple consecutive newlines
           (lambda (match)
             (if (string-match-p "\n\n" match) "\n\n" ""))
           cleaned-output))

    (string-trim cleaned-output)))

(defun org-babel-duckdb-transform-table-section (text)
  "Transform markdown tables in TEXT into org table format.
Converts DuckDB-generated Markdown tables into proper Org tables
by modifying the table structure for compatibility:

1. Identifies separator lines in the markdown table
2. Replaces pipe characters (|) with plus signs (+) in separator lines
3. Removes alignment colons from separator lines
4. Preserves all other table formatting

This creates tables that render correctly in Org mode and
can be manipulated with Org's table editing commands.

TEXT is a string containing markdown-formatted table output.
Returns a string with the transformed table in Org format."
  (let* ((lines (split-string text "\n"))
         (transformed-lines
          (mapcar
           (lambda (line)
             (if (string-match "^\\([ \t]*[|]\\)\\([-|: \t]+\\)\\([|][ \t]*\\)$" line)
                 ;; This is a separator line
                 (let ((prefix (match-string 1 line))
                       (middle (match-string 2 line))
                       (suffix (match-string 3 line)))
                   (concat
                    prefix
                    ;; Replace pipes with plus AND remove colons
                    (replace-regexp-in-string
                     ":" "-"
                     (replace-regexp-in-string "|" "+" middle))
                    suffix))
               ;; Not a separator - keep as is
               line))
           lines)))

    ;; Join with newlines in one operation
    (mapconcat #'identity transformed-lines "\n")))

(defun org-babel-duckdb-transform-output (output)
  "Transform DuckDB OUTPUT by converting markdown tables to org tables.
Post-processes the raw output from DuckDB to convert specially marked sections
into proper Org tables. This works in tandem with the virtual `.mode org-table`
directive implemented by `org-babel-duckdb-insert-org-table-markers`.

The function:
1. Searches for special marker strings:
   - (\"ORG_TABLE_FORMAT_START\" and \"ORG_TABLE_FORMAT_END\")
2. Leaves non-marked sections unchanged
3. Processes marked sections through `org-babel-duckdb-transform-table-section`
4. Reconstructs the output with properly formatted Org tables

This allows users to get native Org tables directly from DuckDB queries
without manual reformatting or additional post-processing steps.

Returns the transformed output with all marked sections converted to Org tables."
  ;; Quick check if transformation is needed
  (if (not (string-match-p "ORG_TABLE_FORMAT_START" output))
      output  ;; No markers, return unchanged

    (let ((result "")
          (pos 0)
          (in-section nil))

      ;; Process the output string in chunks
      (while (string-match "ORG_TABLE_FORMAT_\\(START\\|END\\)" output pos)
        (let* ((match-pos (match-beginning 0))
               (marker-type (match-string 1 output))
               (non-marker-text (substring output pos match-pos))
               (end-marker-pos (+ match-pos (length (match-string 0 output)))))

          ;; Add text before the marker
          (setq result
                (concat result
                        (if in-section
                            ;; Process table separator lines
                            (org-babel-duckdb-transform-table-section non-marker-text)
                          ;; Regular text
                          non-marker-text)))

          ;; Update position and section state
          (setq pos end-marker-pos)
          (setq in-section (string= marker-type "START"))))

      ;; Add any remaining text after the last marker
      (setq result
            (concat result
                    (if in-section
                        ;; This would be a format error (missing END), but handle anyway
                        (org-babel-duckdb-transform-table-section (substring output pos))
                      (substring output pos))))

      result)))

;;; Session Management

(defun org-babel-duckdb-initiate-session (&optional session-name params)
  "Create or reuse a DuckDB session.
SESSION-NAME is the name of the session, and PARAMS are additional parameters.

This function establishes a persistent DuckDB process that:
1. Maintains database state between query executions
2. Preserves temporary tables and variables
3. Connects to the specified database file (if provided)
4. Applies session-wide configuration

If a session with the given name already exists and its process is
alive, that session is reused. Otherwise, a new session is created.
The 'default' session is used when SESSION-NAME is 'yes' or nil."
  (unless (string= session-name "none")
    (let* ((session-name (if (or (null session-name) (string= session-name "yes"))
                            "default" session-name))
           (buffer (org-babel-duckdb-get-session-buffer session-name))
           (db-file (cdr (assq :db params)))
           (process (and (buffer-live-p buffer) (get-buffer-process buffer))))

      ;; Only start a new process if buffer doesn't have a live process
      (unless (and process (process-live-p process))
        (with-current-buffer buffer
          ;; Clear buffer
          (erase-buffer)

          ;; Configure as comint buffer for async support
          (unless (derived-mode-p 'comint-mode)
            (comint-mode)
            (setq-local comint-prompt-regexp "D")
            (setq-local comint-process-echoes t))

          ;; Start the process directly using comint-exec
          (let* ((cmd-args (list org-babel-duckdb-command))
                 (cmd-args (if db-file (append cmd-args (list db-file)) cmd-args)))

            ;; Apply comint-exec to create the process
            (comint-exec buffer
                        (format "duckdb-%s" session-name)
                        (car cmd-args)
                        nil
                        (cdr cmd-args))

            ;; Get the newly created process
            (setq process (get-buffer-process buffer))

            ;; Ensure we have a process
            (unless (and process (process-live-p process))
              (error "Failed to start DuckDB process"))

            ;; Wait for prompt
            (while (not (save-excursion
                          (goto-char (point-min))
                          (re-search-forward "D" nil t)))
              (accept-process-output process 0.1)))))

      ;; Return the session buffer
      buffer)))

;;; Execution Functions

(defun org-babel-duckdb-execute-session (session-buffer body params)
  "Execute DuckDB SQL in SESSION-BUFFER with BODY and PARAMS.
Runs a SQL query in an existing DuckDB session and captures its output.

1. Prepares the session buffer for command input
2. Sends the SQL commands along with necessary dot commands
3. Injects a special marker to detect query completion
4. Implements an adaptive waiting strategy
5. Extracts and cleans the raw output once the marker is found

The adaptive waiting strategy starts with frequent checks (50ms)
for fast queries, gradually increasing wait times for longer-running
queries to allow us to capture the output once finished.

SESSION-BUFFER is the buffer containing the DuckDB process.
BODY is the SQL code to execute.
PARAMS are the header arguments that may contain formatting options.

Returns the raw output of the execution, or signals an error if
the query times out after one minute."
  (let* ((dot-commands (org-babel-duckdb-process-params params))
         (full-body (if dot-commands
                        (concat dot-commands "\n" body)
                      body))
         (process (get-buffer-process session-buffer))
         (completion-marker "DUCKDB_QUERY_COMPLETE_MARKER"))

    (with-current-buffer session-buffer
      ;; Clear buffer from previous output
      (goto-char (point-max))
      (let ((start-point (point)))
        ;; Ensure clean prompt state - if we're not at beginning of line,
        ;; send a newline to get a fresh prompt
        (when (> start-point (line-beginning-position))
          (process-send-string process "\n")
          (accept-process-output process 0.01))

        ;; Start fresh - reset point after potential prompt change
        (goto-char (point-max))
        (setq start-point (point))

        ;; Send command with completion marker to reliably detect
        ;; when the query has finished executing
        (process-send-string process
                             (concat full-body
                                     "\n.print \"" completion-marker "\"\n"))

        ;; Wait for output completion with progressive strategy
        (let ((found nil)
              (timeout 60)    ;; 1 minute timeout
              (wait-time 0.05) ;; Start with 50ms checks
              (total-waited 0))

          ;; Keep waiting until we find the completion marker
          (while (and (not found) (< total-waited timeout))
            ;; Wait for process output - this suspends Emacs until
            ;; new output arrives or the timeout is reached
            (accept-process-output process wait-time)
            (setq total-waited (+ total-waited wait-time))

            ;; Check if marker is in the buffer
            (save-excursion
              (goto-char start-point)
              (when (search-forward completion-marker nil t)
                (setq found t)))

            ;; Progressive waiting strategy - increase wait time as query runs longer
            (cond
             ((< total-waited 1) (setq wait-time 0.05))  ;; First second: check frequently (50ms)
             ((< total-waited 5) (setq wait-time 0.2))   ;; Next few seconds: medium checks (200ms)
             (t (setq wait-time 0.5))))                  ;; After 5 seconds: longer waits (500ms)

          ;; Process the output
          (if found
              (let* ((marker-pos (save-excursion
                                   (goto-char start-point)
                                   (search-forward completion-marker nil t)))
                     ;; Get the line above the marker to exclude the marker itself
                     (output-end (when marker-pos (line-beginning-position 0)))
                     (output (buffer-substring-no-properties
                              start-point output-end)))
                output)
            ;; Handle timeout - signal a clear error
            (error "DuckDB query timed out after %d seconds" timeout)))))))

(defun org-babel-duckdb-execute-sync (session body params)
  "Execute DuckDB code BODY synchronously.
If SESSION is non-nil, execute in that session. PARAMS contains execution options.
Returns the raw output as a string.

For session execution, sends commands to an existing DuckDB process and
captures its output. For non-session execution, uses command-line invocation
with redirected input/output.

This function handles the mechanics of execution but not result processing
or insertion, which is delegated to the calling function."
  (let* ((use-session-p (and session (not (string= session "none"))))
         (db-file (cdr (assq :db params)))
         (temp-in-file (org-babel-duckdb-write-temp-sql body))
         (temp-out-file (org-babel-temp-file "duckdb-out-")))

    (if use-session-p
        ;; Session mode execution
        (let ((session-buffer (org-babel-duckdb-initiate-session session params)))
          (org-babel-duckdb-execute-session session-buffer
                                           (format ".read %s" temp-in-file)
                                           params))

      ;; Direct execution (no session)
      (let ((command (format "%s %s -init /dev/null -batch < %s > %s"
                            org-babel-duckdb-command
                            (or db-file "")
                            temp-in-file
                            temp-out-file)))
        (org-babel-eval command "")
        (with-temp-buffer
          (insert-file-contents temp-out-file)
          (buffer-string))))))

(defun org-babel-duckdb-execute-async (session body params block-id exec-id)
  "Execute the DuckDB code BODY asynchronously in SESSION.
Uses PARAMS for execution options and identifies the source by BLOCK-ID and EXEC-ID.
Immediately updates the source block with a placeholder and then starts execution.

This function implements non-blocking execution by:
1. Creating unique marker strings to delimit this execution's output
2. Installing a process filter that captures output between these markers
3. Setting up a timer to check for execution completion
4. Processing output once the execution completes
5. Updating the source block with the results

SESSION is the session name for connecting to DuckDB.
BODY contains the SQL statements to execute.
PARAMS are the block's header arguments and options.
BLOCK-ID and EXEC-ID identify the source block and execution context.

Returns nil, as the result handling happens asynchronously via process filter."
  (message "[ob-duckdb] Starting async execution for block %s" (substring block-id 0 8))

  ;; First, update the source block with a placeholder
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

  ;; Create a unique marker for this execution
  (let* ((start-marker (format "DUCKDB_START_%s" exec-id))
         (end-marker (format "DUCKDB_END_%s" exec-id))
         (temp-in-file (org-babel-duckdb-write-temp-sql body))
         (session-buffer (org-babel-duckdb-initiate-session session params))
         (output-buffer (generate-new-buffer (format "*duckdb-async-output-%s*" exec-id)))
         (result-params (cdr (assq :result-params params))))

    ;; Add comint output filter to capture all output
    (let ((process (get-buffer-process session-buffer))
          (complete nil)
          (capturing nil))

      ;; Create a process filter that will capture output between our markers
      (let ((original-filter (process-filter process))
            (buffer output-buffer))

        (set-process-filter
         process
         (lambda (proc string)
           ;; Check for start marker
           (when (and (not capturing) (string-match-p start-marker string))
             (setq capturing t))

           ;; Capture output while between markers
           (when capturing
             (with-current-buffer buffer
               (goto-char (point-max))
               (insert string))

             ;; Check for end marker
             (when (string-match-p end-marker string)
               (setq capturing nil)
               (setq complete t)))

           ;; Call original filter to maintain normal behavior
           (when original-filter
             (funcall original-filter proc string))))

        ;; Send the command to the process
        (process-send-string
         process
         (format "\n.print \"%s\"\n.read %s\n.print \"%s\"\n"
                 start-marker
                 temp-in-file
                 end-marker))

        ;; Wait for a short while to ensure the command starts executing
        (sleep-for 0.05)

        ;; Store the timer in a lexical variable that will be captured by the lambda
        (let ((timer-self nil))
          (setq timer-self
                (run-with-timer
                 0.1 0.1
                 (lambda ()
                   (when complete
                     ;; Cancel this timer first
                     (when timer-self
                       (cancel-timer timer-self))

                     ;; Process the captured output
                     (with-current-buffer buffer
                       (let* ((raw-output (buffer-string))
                              ;; Remove the markers
                              (marker-cleaned
                               (replace-regexp-in-string
                                (format "\\(%s\\|%s\\)"
                                        (regexp-quote start-marker)
                                        (regexp-quote end-marker))
                                ""
                                raw-output)))

                         ;; Process the result through the same pipeline as sync
                         (org-babel-duckdb-process-and-update-result
                          block-id marker-cleaned params result-params)))

                     ;; Restore original filter and clean up
                     (set-process-filter process original-filter)
                     (kill-buffer buffer))))))

        ;; Return nil - we've already handled the placeholder update
        nil))))

(defun org-babel-duckdb-process-and-update-result (block-id raw-output params result-params)
  "Process RAW-OUTPUT and update result for the block with BLOCK-ID.
This function is called both for sync and async execution paths,
ensuring consistent processing of results.

The function:
1. Cleans the raw output by removing DuckDB artifacts
2. Transforms any special format sections (e.g., org tables)
3. Identifies the source block in its current position
4. Handles buffered output if requested
5. Updates the source block with the processed results

BLOCK-ID uniquely identifies the source block to update.
RAW-OUTPUT is the unprocessed output string from DuckDB.
PARAMS contains all header arguments from the block.
RESULT-PARAMS controls how results should be formatted and inserted.

Returns the processed output string for potential further use."
  (message "[ob-duckdb] Processing results for block %s" (substring block-id 0 8))

  ;; Process the raw output
  (let* ((cleaned-output (org-babel-duckdb-clean-output raw-output))
         (transformed-output (org-babel-duckdb-transform-output cleaned-output))
         (output-type (cdr (assq :output params)))
         (use-buffer-output (and output-type (string= output-type "buffer"))))

    ;; Now update the source block with the processed result
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

                ;; Handle output to buffer if requested
                (if use-buffer-output
                    (progn
                      (org-babel-remove-result)
                      (org-babel-insert-result "Output sent to buffer." effective-params)
                      (org-babel-duckdb-display-buffer transformed-output))

                  ;; Normal output in source block
                  (progn
                    ;; Add wrap to params if needed
                    (when (and wrap (not (assq :wrap effective-params)))
                      (setq effective-params
                            (cons (cons :wrap wrap) effective-params)))

                    ;; Process result for ANSI colors
                    (when (and (stringp transformed-output) (string-match-p "\e\\[" transformed-output))
                      (with-temp-buffer
                        (insert transformed-output)
                        (ansi-color-apply-on-region (point-min) (point-max))
                        (setq transformed-output (buffer-string))))

                    ;; Insert the result
                    (condition-case err
                        (progn
                          (org-babel-remove-result)
                          (org-babel-insert-result
                           (if (member "table" result-params)
                               (org-babel-duckdb-table-or-scalar transformed-output)
                             transformed-output)
                           effective-params)

                          ;; Process ANSI colors in the result region
                          (when-let ((result-pos (org-babel-where-is-src-block-result)))
                            (save-excursion
                              (goto-char result-pos)
                              (when (looking-at org-babel-result-regexp)
                                (let ((end (org-babel-result-end))
                                      (ansi-color-context-region nil))
                                  (ansi-color-apply-on-region result-pos end))))))
                      (error
                       (message "[ob-duckdb] Error updating result: %S" err)))))))))))

    ;; Return the processed output for potential further use
    transformed-output))

(defun org-babel-duckdb-table-or-scalar (result)
  "Convert RESULT into an appropriate Elisp value for Org table.
This function is only used when :results table is explicitly specified.
Attempts to parse the result as an Org-compatible table structure by:

1. Splitting the result into lines
2. Checking for table-like structure with separator lines
3. Parsing the header row and data rows into a cons cell structure
   with (header . data-rows) format

Limitations: Works best with simple tabular formats like ASCII and markdown.
Complex formats may not parse correctly and will be returned as raw strings.

Returns either a cons cell with (header . data-rows) or the original string
if it cannot be parsed as a table."
  (let ((lines (split-string result "\n" t)))
    (if (and (> (length lines) 1)
             (string-match "^[-+|]" (nth 1 lines)))
        ;; This is a table, process it
        (let* ((header (car lines))
               (separator (nth 1 lines))
               (data (cddr lines)))
          (cons (split-string header "|" t)
                (mapcar (lambda (row) (split-string row "|" t)) data)))
      ;; Not a table, return as is
      result)))

(defun org-babel-duckdb-display-buffer (output)
  "Display OUTPUT in a dedicated buffer.
Creates or reuses the buffer specified by `org-babel-duckdb-output-buffer',
clears its content, inserts the query output, processes any ANSI color
codes, and displays the buffer.

This function is particularly useful for:
1. Viewing large result sets without cluttering the Org document
2. Preserving colorized output that might be lost in the Org buffer
3. Examining results in a separate window while continuing to edit
4. Temporary display of results without modifying the document

The buffer's contents are replaced each time this function is called,
showing only the most recent output."
  (let ((buf (get-buffer-create org-babel-duckdb-output-buffer)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert output)
        (ansi-color-apply-on-region (point-min) (point-max))
        (goto-char (point-min))))
    (display-buffer buf)
    ;; Return output for further processing if needed
    output))

;;; Session Management Functions

(defun org-babel-duckdb-list-sessions ()
  "List all active DuckDB sessions.
Examines the session registry to find all currently running DuckDB
sessions managed by this package.

Returns an alist of (session-name . buffer) pairs that can be used
to access or manipulate the sessions programmatically.

This function is useful for:
1. Discovering what sessions exist
2. Programmatic access to session buffers
3. Debugging session-related issues
4. Building interfaces for session management"
  (let (sessions)
    (maphash (lambda (name buffer)
               (push (cons name buffer) sessions))
             org-babel-duckdb-sessions)
    (nreverse sessions)))

(defun org-babel-duckdb-create-session (session-name &optional db-file)
  "Create a new DuckDB session named SESSION-NAME.
Initializes a new DuckDB process with the given name. This is useful
for setting up persistent connections outside of source code blocks.

If DB-FILE is provided, connect to that database file.
The session remains active until explicitly deleted or until Emacs exits.

Returns the session buffer containing the DuckDB process.

This provides a convenient way to:
1. Set up named sessions for later use in source blocks
2. Establish database connections interactively
3. Create debugging sessions separate from document execution
4. Pre-initialize sessions with specific database files"
  (interactive "sSession name: \nfDatabase file (optional): ")
  (let ((params (if db-file (list (cons :db db-file)) nil)))
    (org-babel-duckdb-initiate-session session-name params)))

(defun org-babel-duckdb-delete-session (session-name)
  "Delete the DuckDB session named SESSION-NAME.
This will terminate the DuckDB process and remove the session
from `org-babel-duckdb-sessions'.

The function performs a clean shutdown of the session:
1. Sends an exit command to the DuckDB process
2. Allows a brief period for the process to terminate
3. Forcefully kills the process if it doesn't exit cleanly
4. Kills the associated buffer
5. Removes the session from the registry

This ensures resources are properly released and prevents
accumulation of zombie processes or buffers."
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

(defun org-babel-duckdb-display-sessions ()
  "Display information about all active DuckDB sessions in a buffer.
Creates a formatted report showing details about each session, including:
- Session name
- Database connection (if available)
- Process status
- Buffer information

This provides a user-friendly way to see what sessions exist
and their current state, which is useful for session management
and debugging session-related issues."
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
                 (db-file "N/A"))  ;; Could extract DB file from process cmd
            (princ (format "%-20s %-25s %-10s\n"
                           name
                           db-file
                           status))))))))

;;; Debug Utilities

(defun org-babel-duckdb-debug-show-process-buffer (session-name)
  "Show the process buffer for SESSION-NAME to examine raw output.
This is a debugging utility that displays the comint buffer where the
DuckDB process is running. This lets you see what's being sent to and
received from DuckDB, which can help diagnose communication issues."
  (interactive "sSession name: ")
  (let ((buffer (gethash session-name org-babel-duckdb-sessions)))
    (if buffer
        (display-buffer buffer)
      (message "No session buffer found for %s" session-name))))

(defun org-babel-duckdb-debug-recent-outputs ()
  "Display recent captured outputs from async executions.
Creates a buffer collecting all temporary output buffers created during
async execution. This provides a view of what's been captured from
recent query executions, which can help diagnose issues with output
capturing, parsing, or result processing.

The function searches for all buffers matching the temporary output
buffer naming pattern and displays their contents in a diagnostic
buffer."
  (interactive)
  (with-output-to-temp-buffer "*DuckDB Async Debug*"
    (let ((buffers (buffer-list)))
      (dolist (buf buffers)
        (when (string-match-p "\\*duckdb-async-output-" (buffer-name buf))
          (princ (format "=== Buffer: %s ===\n" (buffer-name buf)))
          (with-current-buffer buf
            (princ (buffer-string)))
          (princ "\n\n"))))))

;;; Registration Capture

(defun org-babel-duckdb-capture-registration-advice (result)
  "Advice function to capture block registration info.
This function is added as :filter-return advice to
`org-duckdb-blocks-register-execution', capturing the block topology
information needed for result routing.

When a DuckDB block is about to be executed, this advice intercepts the
registration result (containing block and execution IDs) and stores it
in `org-babel-duckdb-last-registration'. This allows the execution
function to know which block it's processing.

RESULT is the plist returned by `org-duckdb-blocks-register-execution',
containing at minimum :block-id and :exec-id keys."
  (when result
    (setq org-babel-duckdb-last-registration result))
  result)

;;; Main Execution Function

(defun org-babel-execute:duckdb (body params)
  "Execute a block of DuckDB SQL code with PARAMS.
This is the main entry point for executing DuckDB source blocks in Org Babel.
It handles the entire execution workflow:

1. Initializes block tracking (via org-duckdb-blocks)
2. Processes parameters and session requirements
3. Determines synchronous vs asynchronous execution mode
4. Expands variables and prepares the query
5. Delegates to the appropriate execution function
6. Processes and formats the results
7. Returns results in the appropriate format for Babel

BODY is the SQL code content from the source block.
PARAMS is an alist of header arguments controlling execution behavior.

Returns the execution results formatted according to PARAMS[:result-params]."
  (message "[ob-duckdb] Starting execution")

  ;; Ensure tracking system is initialized
  (unless (advice-member-p 'org-duckdb-blocks-register-execution 'org-babel-execute-src-block)
    (org-duckdb-blocks-setup))

  ;; Common setup for both sync and async execution
  (let* ((session (cdr (assq :session params)))
         (use-async (and (cdr (assq :async params))
                         (string= (cdr (assq :async params)) "yes")))
         (result-params (cdr (assq :result-params params))))

    ;; Verify async requirements
    (when (and use-async
               (or (null session) (string= session "none")))
      (user-error "[ob-duckdb] Asynchronous execution requires a session. Please add ':session name' to your header arguments"))

    ;; Continue with expanded body and execution
    (let ((expanded-body (org-babel-expand-body:duckdb body params)))
      ;; Prepare dot commands and full body
      (let* ((dot-commands (org-babel-duckdb-process-params params))
             (combined-body (if dot-commands
                                (concat dot-commands "\n" expanded-body)
                              expanded-body))
             (marked-body (org-babel-duckdb-insert-org-table-markers combined-body)))

        ;; Execute differently based on sync/async
        (if use-async
            (progn
              ;; Get block IDs from registration info
              (unless org-babel-duckdb-last-registration
                (error "No block registration info available"))

              (let ((block-id (plist-get org-babel-duckdb-last-registration :block-id))
                    (exec-id (plist-get org-babel-duckdb-last-registration :exec-id)))

                ;; Reset registration after retrieving it
                (setq org-babel-duckdb-last-registration nil)

                ;; Execute asynchronously - it handles placeholder directly
                (org-babel-duckdb-execute-async session marked-body params block-id exec-id)
                "Executing asynchronously..."))

          ;; Execute synchronously
          (let* ((raw-result (org-babel-duckdb-execute-sync session marked-body params))
                 (block-id nil)
                 (exec-id nil))

            ;; Try to get block/exec IDs from registration if available
            (when org-babel-duckdb-last-registration
              (setq block-id (plist-get org-babel-duckdb-last-registration :block-id)
                    exec-id (plist-get org-babel-duckdb-last-registration :exec-id))
              (setq org-babel-duckdb-last-registration nil))

            ;; Process results
            (if (and block-id exec-id)
                ;; If we have block IDs, use the same process-and-update function
                (org-babel-duckdb-process-and-update-result block-id raw-result params result-params)

              ;; Otherwise (non-tracked execution), process directly and return
              (let* ((cleaned-output (org-babel-duckdb-clean-output raw-result))
                     (transformed-output (org-babel-duckdb-transform-output cleaned-output))
                     (output-type (cdr (assq :output params)))
                     (use-buffer-output (and output-type (string= output-type "buffer"))))

                ;; Handle output to buffer if requested
                (if use-buffer-output
                    (progn
                      (org-babel-duckdb-display-buffer transformed-output)
                      "Output sent to buffer.")

                  ;; Return appropriately processed results
                  (if (member "table" result-params)
                      (org-babel-duckdb-table-or-scalar transformed-output)
                    transformed-output))))))))))

;;; Language Integration

;; Add duckdb to babel languages
(add-to-list 'org-babel-tangle-lang-exts '("duckdb" . "sql"))
(add-to-list 'org-src-lang-modes '("duckdb" . sql))

;; Support for ANSI color in output
(defun org-babel-duckdb-babel-ansi ()
  "Process ANSI color codes in the latest Babel result.
This converts ANSI color escape sequences to text properties,
allowing for colored output in the results.

The function:
1. Locates the most recent Babel result section
2. Checks if it matches the expected result format
3. Determines the boundaries of the result region
4. Applies ANSI color processing to that region only

This function is added to `org-babel-after-execute-hook' and runs
automatically after each source block execution, ensuring that
colorized output from DuckDB (like error messages, warnings, and
specially formatted data) appears correctly in the Org buffer."
  (when-let ((beg (org-babel-where-is-src-block-result nil nil)))
    (save-excursion
      (goto-char beg)
      (when (looking-at org-babel-result-regexp)
        (let ((end (org-babel-result-end))
              (ansi-color-context-region nil))
          (ansi-color-apply-on-region beg end))))))

;; Add advice to capture registration info
(advice-add 'org-duckdb-blocks-register-execution :filter-return
            #'org-babel-duckdb-capture-registration-advice)

;; Set up babel integration when the file is loaded
(add-hook 'org-babel-after-execute-hook 'org-babel-duckdb-babel-ansi)

;; Initialize the tracking system
(org-duckdb-blocks-setup)

(provide 'ob-duckdb)

;;; ob-duckdb.el ends here
