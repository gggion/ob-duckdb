;;; ob-duckdb.el --- Org Babel functions for DuckDB SQL -*- lexical-binding: t; -*-

;; Author: gggion
;; Maintainer: gggion
;; Version: 1.0.0
;; Package-Requires: ((emacs "28.1") (org "9.4"))
;; Keywords: languages, org, babel, duckdb, sql
;; URL: https://github.com/gggion/ob-duckdb.el

;;; Commentary:
;;
;; Org-Babel support for DuckDB SQL evaluations within Org mode buffers.
;; DuckDB is a high-performance analytical database system designed for
;; in-process data analytics that combines insanely fast analytical
;; performance with a friendly SQL interface.
;;
;; Features of this package include:
;;
;; - Execute DuckDB SQL queries directly in Org source blocks
;; - Variable substitution with standard Org Babel :var header arguments
;; - Persistent sessions for interactive query development
;; - Result formatting via DuckDB dot commands
;; - Special `.mode org-table` format that creates native Org tables
;; - Colorized output and special buffer display options
;; - Database connection via the :db header argument
;;
;; Basic usage example:
;;
;; #+begin_src duckdb :db mydata.duckdb
;;   SELECT * FROM mytable LIMIT 10;
;; #+end_src
;;
;; Output formatting example with native Org tables:
;;
;; #+begin_src duckdb :db mydata.duckdb
;;   .mode org-table
;;   SELECT * FROM mytable LIMIT 10;
;; #+end_src
;;
;; Header arguments:
;; :db        Path to DuckDB database file
;; :session   Name of session for persistent connections
;; :format    DuckDB output format (table, csv, json, org-table, etc.)
;; :separator Column separator for output
;; :headers   Whether to include column headers (on/off)
;; :timer     Show execution time (on/off)
;; :output    Output destination ("buffer" for dedicated buffer)
;;
;; For more details, see the Org-mode manual.

;;; Code:

;; TODO: autocompletion on src blocks
;; Dependencies
(require 'org-macs)
(require 'ob)
(require 'ob-ref)
(require 'ob-comint)
(require 'ob-eval)
(require 'ansi-color)

;;; Language Registration

;; Define the file extension for tangling duckdb blocks
(add-to-list 'org-babel-tangle-lang-exts '("duckdb" . "sql"))

;; Default header arguments for duckdb blocks
(defvar org-babel-default-header-args:duckdb
  '((:results . "output")
    (:wrap))
  "Default header arguments for duckdb code blocks.
By default, uses `output' with `:wrap' to preserve formatting.")

(defconst org-babel-header-args:duckdb
  '((db        . :any)  ; Database file to use
    (format    . :any)  ; Output format (csv, table, json, etc.)
    (timer     . :any)  ; Show execution time
    (headers   . :any)  ; Show column headers
    (nullvalue . :any)  ; String to use for NULL values
    (separator . :any)  ; Column separator
    (echo      . :any)  ; Echo commands
    (bail      . :any)  ; Exit on error
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
output:    Control result display (\"buffer\" for dedicated output)")

;;; Customization Options

(defcustom org-babel-duckdb-command "duckdb"
  "Command used to execute DuckDB.
This should be the path to the DuckDB executable or simply \"duckdb\"
if it's in your PATH. You can also include command-line arguments
that should be used for all DuckDB executions."
  :type 'string
  :group 'org-babel)

(defcustom org-babel-duckdb-output-buffer "*DuckDB-output*"
  "Buffer name for displaying DuckDB query results.
This buffer is used when the :output header argument is set to \"buffer\".
Results will be displayed in this buffer with ANSI color processing applied."
  :type 'string
  :group 'org-babel)

;;; Session Management

(defvar org-babel-duckdb-sessions (make-hash-table :test 'equal)
  "Hash table of active DuckDB session buffers.
Keys are session names and values are the corresponding buffer objects.
This allows multiple independent DuckDB sessions to be managed simultaneously.")

(defun org-babel-duckdb-cleanup-sessions ()
  "Clean up dead DuckDB sessions from the session registry.
This removes sessions whose processes or buffers no longer exist."
  (interactive)
  (maphash
   (lambda (name buffer)
     (when (or (not (buffer-live-p buffer))
               (not (process-live-p (get-buffer-process buffer))))
       (remhash name org-babel-duckdb-sessions)))
   org-babel-duckdb-sessions))

(defun org-babel-duckdb-get-session-buffer (session-name)
  "Get or create a buffer for the DuckDB SESSION.
If a buffer for the named session already exists, return it.
Otherwise create a new buffer and register it in `org-babel-duckdb-sessions'.
Returns a buffer object dedicated to the named session."
  (let ((buffer-name (format "*DuckDB:%s*" session-name)))
    (or (gethash session-name org-babel-duckdb-sessions)
        (let ((new-buffer (get-buffer-create buffer-name)))
          (puthash session-name new-buffer org-babel-duckdb-sessions)
          new-buffer))))

;;; Variable Handling
;; BUG: Currently not working with lists (but table as variable does)
(defun org-babel-duckdb-var-to-duckdb (var)
  "Convert an Emacs Lisp value VAR to a DuckDB SQL value.
Handles various data types:
- Lists are converted to DuckDB arrays or VALUES expressions
- Org table horizontal separators ('hline) become NULL
- Strings are properly quoted with single quotes
- nil values become NULL
- Other values are converted to their string representation"
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
need to be explicitly assigned in the DuckDB session."
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


;; BUG:(OR FEATURE?) variable replacement works, but it replaces every instance
;; of variable name, even inside another word example: :var my = 'test' will
;; transform a string like my_table into test_table or "my_cool_column_name"
;; into test_cool_column_name
;;
;; This occurs because the current implementation uses a basic word boundary (\b) regexp
;; which matches variable names that are part of larger identifiers. A more selective
;; approach would require context-aware SQL parsing to identify valid substitution points.
;;
;; TODO: make it so we dont replace duckdb/sql keywords (could probabaly use duckdb_keywords() to fetch all)
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
  "Process header parameters and generate appropriate DuckDB dot commands.
Converts header arguments to their corresponding DuckDB configuration commands:
- :format → .mode (output format)
- :timer → .timer (execution timing)
- :headers → .headers (column headers display)
- :nullvalue → .nullvalue (NULL representation)
- :separator → .separator (column delimiter)
- :echo → .echo (command echo)
- :bail → .bail (error handling)

Returns a string of newline-separated dot commands to configure DuckDB."
  (let ((format (cdr (assq :format params)))
        (timer (cdr (assq :timer params)))
        (headers (cdr (assq :headers params)))
        (nullvalue (cdr (assq :nullvalue params)))
        (separator (cdr (assq :separator params)))
        (echo (cdr (assq :echo params)))
        (bail (cdr (assq :bail params))))

    ;; Use with-temp-buffer for string building which benchmarks showed was fastest
    (with-temp-buffer
      ;; Add each command if its parameter is specified
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
uses `org-babel-temp-file' to ensure proper file management."
  (let ((temp-file (org-babel-temp-file "duckdb-")))
    (with-temp-file temp-file
      (insert body))
    temp-file))

(defun org-babel-duckdb-clean-output (output)
  "Clean DuckDB output by removing unnecessary lines and formatting.
Processes raw DuckDB OUTPUT to remove:
- Version information and connection banners
- Progress bars
- Prompt characters
- Process status messages
- Excess whitespace and blank lines"
  (let ((cleaned-output
         (replace-regexp-in-string
          (rx (or
               ;; Prompt characters
               (seq bol "D" (+ (any " ·")))

               ;; Process termination message
               (seq bol "Process duckdb finished" eol)

               ;; Opening file instructions
               (seq bol "Use \".open FILENAME\"" (zero-or-more not-newline) "\n")

               ;; Progress bars
               (seq bol (zero-or-more space)
                    (one-or-more digit) "% ▕" (zero-or-more (any "█" " ")) "▏"
                    (zero-or-more not-newline) eol)

               ;; Marker lines
               (seq bol "marker" eol)

               ;; Config lines (echo, headers, mode)
               (seq bol (zero-or-more space) "echo:" (zero-or-more not-newline) "\n"
                    (zero-or-more space) "headers:" (zero-or-more not-newline) "\n"
                    (zero-or-more space) "mode:" (zero-or-more not-newline) "\n")
               ))
          ""
          output)))
    ;; Trim excess blank lines at the beginning and end
    (string-trim cleaned-output)))

(defun org-babel-duckdb--transform-table-section (text)
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
3. Processes marked sections through `org-babel-duckdb--transform-table-section`
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
                            (org-babel-duckdb--transform-table-section non-marker-text)
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
                        (org-babel-duckdb--transform-table-section (substring output pos))
                      (substring output pos))))

      result)))



;;; Execution Functions
;; NOTE: Not used, but it might come in handy in the future as a parameter in order to use files to optimize memory usage
(defun org-babel-duckdb-execute-with-file (body db-file &optional dot-commands)
  "Execute DuckDB SQL in BODY using a temporary file.
This is for one-off (non-session) query execution.

When DB-FILE is provided, connect to that database.
DOT-COMMANDS are prepended to BODY to configure the DuckDB environment.

This function writes the query to a temporary file, executes it with
the DuckDB command line tool, and returns the raw output."
  (let* ((sql-file (org-babel-duckdb-write-temp-sql
                    (if dot-commands
                        (concat dot-commands "\n" body)
                      body)))
         (cmd (if db-file
                  (format "%s %s -init /dev/null -batch < %s"
                          org-babel-duckdb-command db-file sql-file)
                (format "%s -init /dev/null -batch < %s"
                        org-babel-duckdb-command sql-file))))
    (org-babel-eval cmd "")))

(defun org-babel-duckdb-initiate-session (&optional session-name params)
  "Create or reuse a DuckDB session.
SESSION-NAME is the name of the session (defaults to \"default\").
PARAMS are the header arguments from the source block."
  (unless (string= session-name "none")
    (let* ((session-name (if (string= session-name "yes") "default" session-name))
           (session-buffer (org-babel-duckdb-get-session-buffer session-name))
           (db-file (cdr (assq :db params)))
           (process (get-buffer-process session-buffer)))

      ;; Only start a new process if needed
      (unless (and process (process-live-p process))
        (with-current-buffer session-buffer
          (erase-buffer)
          (message "Starting new DuckDB session: %s" session-name)

          ;; Start process
          (let* ((cmd-args (list org-babel-duckdb-command))
                 (cmd-args (if db-file (append cmd-args (list db-file)) cmd-args))
                 (process (apply #'start-process
                                (format "duckdb-%s" session-name)
                                session-buffer
                                cmd-args)))

            ;; Wait for prompt
            (while (not (save-excursion
                          (goto-char (point-min))
                          (re-search-forward "^D " nil t)))
              (accept-process-output process 0.1))

            ;; Send a test query to ensure connection is ready
            (process-send-string process "SELECT 1 AS test;\n")
            (accept-process-output process 0.2))))

      ;; Return the buffer
      session-buffer)))

(defun org-babel-prep-session:duckdb (session params)
  "Prepare SESSION according to the header arguments in PARAMS.
This function sets up a DuckDB session with specified variables.
It creates and initializes the session if it doesn't exist, then
defines any variables specified in PARAMS within the session.

Returns the prepared session buffer."
  (let* ((session (org-babel-duckdb-initiate-session session params))
	 (var-lines
	  (org-babel-variable-assignments:duckdb params)))
    (org-babel-comint-in-buffer session
      (mapc (lambda (var)
              (insert var)
              (comint-send-input)
              (org-babel-comint-wait-for-output session))
	    var-lines))
    session))

(defun org-babel-load-session:duckdb (session body params)
  "Load BODY into SESSION.
This prepares the session and loads the SQL code for execution.
Creates the session if it doesn't exist, then inserts and sends
the expanded body code to the session process.

Returns the session buffer with the code loaded and ready."
  (save-window-excursion
    (let ((buffer (org-babel-prep-session:duckdb session params)))
      (with-current-buffer buffer
        (goto-char (process-mark (get-buffer-process (current-buffer))))
        (insert (org-babel-expand-body:duckdb body params))
        (comint-send-input))
      buffer)))

;;; Result Processing
;; TODO: not functioning properly for all .mode formats, I'll need to apply a
;; more thorough conversion method (vectors?)
;;
;; Current implementation has limitations with complex table formats. A more robust
;; approach would need format-specific parsers for each DuckDB output mode.
;; Particularly challenging are formats like 'json' or 'parquet' which require
;; specialized parsing logic.
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

NOTE: This function should only be called when a table result
is explicitly requested via :results table header argument."
  (let ((lines (split-string result "\n" t)))
    (if (and (> (length lines) 1)
             (string-match "^[-+|]" (nth 1 lines))) ; Look for table separator
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
codes, and displays the buffer."

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

(defun org-babel-duckdb-execute-session (session-buffer body params)
  "Execute DuckDB SQL in SESSION-BUFFER with BODY and PARAMS.
Runs a SQL query in an existing DuckDB session and captures its output.

This function handles the intricacies of asynchronous process interaction:
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

Returns the cleaned output of the execution, or signals an error if
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
                ;; Clean up and return the output
                (org-babel-duckdb-clean-output output))
            ;; Handle timeout - signal a clear error
            (error "DuckDB query timed out after %d seconds" timeout)))))))


;;; Session Management Functions
;; These functions provide a high-level interface for working with DuckDB sessions
;; interactively from Emacs. They allow creating, listing, and deleting sessions
;; from both Lisp code and interactive commands.
(defun org-babel-duckdb-list-sessions ()
  "List all active DuckDB sessions.
Examines the session registry to find all currently running DuckDB
sessions managed by this package.

Returns an alist of (session-name . buffer) pairs that can be used
to access or manipulate the sessions programmatically."
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

Returns the session buffer containing the DuckDB process."
  (interactive "sSession name: \nfDatabase file (optional): ")
  (let ((params (if db-file (list (cons :db db-file)) nil)))
    (org-babel-duckdb-initiate-session session-name params)))

(defun org-babel-duckdb-delete-session (session-name)
  "Delete the DuckDB session named SESSION-NAME.
This will terminate the DuckDB process and remove the session
from `org-babel-duckdb-sessions'."
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
  "Display information about all active DuckDB sessions in a buffer."
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



;;; Main Execution Function

(defun org-babel-execute:duckdb (body params)
  "Execute a block of DuckDB SQL code with PARAMS.
This is the main entry point called by `org-babel-execute-src-block'.

BODY contains the SQL code to be executed.
PARAMS contains the header arguments for the source block.

The execution process follows these steps:
1. Expand variable references in the query body
2. Generate DuckDB dot commands from header arguments
3. Apply special transformations like org-table markers
4. Execute the query either in session or direct mode
5. Process the raw output (clean and transform)
6. Format results according to result-params

The function handles various execution modes:
- Session vs. non-session execution
- Variable substitution and expansion
- Formatting based on result parameters
- Output buffer display options
- Special org-table transformation

Returns the query results in the format specified by result-params."

  (let* ((session (cdr (assq :session params)))
         (db-file (cdr (assq :db params)))
         (result-params (cdr (assq :result-params params)))
         (out-file (cdr (assq :out-file params)))
         (output-type (cdr (assq :output params)))
         (use-buffer-output (and output-type (string= output-type "buffer")))
         (cmdline (cdr (assq :cmdline params)))
         (colnames-p (not (equal "no" (cdr (assq :colnames params)))))
         (expanded-body (org-babel-expand-body:duckdb body params))
         (use-session-p (and session (not (string= session "none"))))
         (temp-out-file (or out-file (org-babel-temp-file "duckdb-out-")))
         (temp-in-file (org-babel-temp-file "duckdb-in-"))
         raw-result)

    ;; Create DuckDB SQL script file with proper commands
    (with-temp-file temp-in-file
      (let* ((dot-commands (org-babel-duckdb-process-params params))
             ;; Combine dot commands with expanded body
             (combined-content (if dot-commands
                                   (concat dot-commands "\n" expanded-body)
                                 expanded-body))
             ;; Apply org-table markers to the whole script
             ;; This transforms any `.mode org-table` directives into a special
             ;; format that will be post-processed after execution
             (marked-content (org-babel-duckdb-insert-org-table-markers combined-content)))
        (insert marked-content)
        ;; Add .exit to ensure clean exit
        (insert "\n.exit\n")))

    ;; Execute DuckDB - either in session or direct mode
    (setq raw-result
          (if use-session-p
           ;; Session mode - uses a persistent DuckDB process
           (let ((session-buffer (org-babel-duckdb-initiate-session session params)))
                (with-current-buffer session-buffer
                  ;; Clear any previous results
                  (goto-char (point-max))
                  (let ((start-point (point))
                        (process (get-buffer-process session-buffer))
                        (completion-marker "DUCKDB_QUERY_COMPLETE"))

                    ;; Ensure we're at a clean prompt before starting
                    (unless (bolp)
                      (process-send-string process "\n")
                      (accept-process-output process 0.1))

                    ;; Reset start point to current process position
                    (goto-char (point-max))
                    (setq start-point (point))

                    ;; Read and execute commands from temp file
                    (process-send-string
                     process
                     (format ".read %s\n.print \"%s\"\n"
                             temp-in-file completion-marker))

                    ;; Wait for completion with progressive timeouts
                    (let ((found nil)
                          (timeout 60)
                          (wait-time 0.05)
                          (total-waited 0))
                      (while (and (not found) (< total-waited timeout))
                        (accept-process-output process wait-time)
                        (setq total-waited (+ total-waited wait-time))

                        ;; Check if our marker is in the output
                        (save-excursion
         (goto-char start-point)
         (when (search-forward completion-marker nil t)
                            (setq found t)))

                        ;; Adaptive wait times
                        (setq wait-time (min 1.0 (* wait-time 1.5))))

                      ;; Extract the output
                      (if found
           (let* ((marker-pos (save-excursion
         (goto-char start-point)
         (search-forward completion-marker nil t)
         (line-beginning-position)))
                                 (output (buffer-substring-no-properties
                                          start-point marker-pos)))
                            (org-babel-duckdb-clean-output output))
                        (error "DuckDB query timed out after %d seconds" timeout))))))

            ;; Non-session mode - direct file execution
            (let ((command (format "%s %s -init /dev/null -batch < %s > %s"
                                   org-babel-duckdb-command
                                   (or db-file "")
                                   temp-in-file
                                   temp-out-file)))
              (org-babel-eval command "")
              (with-temp-buffer
         (insert-file-contents temp-out-file)
         (org-babel-duckdb-clean-output (buffer-string))))))

    ;; Transform markdown tables to org tables in marked sections
    ;; This converts any sections previously marked with the virtual
    ;; `.mode org-table` format into proper Org-compatible tables
    (setq raw-result (org-babel-duckdb-transform-output raw-result))

    ;; Handle output to buffer if requested
    (when use-buffer-output
      (org-babel-duckdb-display-buffer raw-result))

    ;; Process the results according to params
    (if use-buffer-output
        ;; If output is directed to a buffer, still return simple results
        "Output sent to buffer."

      ;; Check if table results are explicitly requested
      (if (member "table" result-params)
          (org-babel-duckdb-table-or-scalar raw-result)
        ;; Otherwise, return raw result with appropriate formatting
        (org-babel-result-cond result-params
          raw-result  ;; Raw string
          raw-result))))) ;; Still raw - no parsing

;;; Language Integration

;; Add duckdb to babel languages
(add-to-list 'org-babel-tangle-lang-exts '("duckdb" . "sql"))
(add-to-list 'org-src-lang-modes '("duckdb" . sql))

;; Support for ANSI color in output
(defun org-babel-duckdb-babel-ansi ()
  "Process ANSI color codes in the latest Babel result.
This converts ANSI color escape sequences to text properties,
allowing for colored output in the results.

This function is added to `org-babel-after-execute-hook'."
  (when-let ((beg (org-babel-where-is-src-block-result nil nil)))
    (save-excursion
      (goto-char beg)
      (when (looking-at org-babel-result-regexp)
        (let ((end (org-babel-result-end))
              (ansi-color-context-region nil))
          (ansi-color-apply-on-region beg end))))))

;; Set up babel integration when the file is loaded
(add-hook 'org-babel-after-execute-hook 'org-babel-duckdb-babel-ansi)

(provide 'ob-duckdb)
;;; ob-duckdb.el ends here
