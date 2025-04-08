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
;; - Colorized output and special buffer display options
;; - Database connection via the :db header argument
;;
;; Basic usage example:
;;
;; #+begin_src duckdb :db mydata.duckdb
;;   SELECT * FROM mytable LIMIT 10;
;; #+end_src
;;
;; :db        Path to DuckDB database file
;; :session   Name of session for persistent connections
;; :format    DuckDB output format (table, csv, json, etc.)
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
(defvar org-babel-default-header-args:duckdb '()
  "Default header arguments for duckdb code blocks.
You can customize this to set defaults like :format or :timer.")

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


;; BUG:(OR FEATURE?) variable replacement works, but it replaces every instance
;; of variable name, even inside another word example: :var my = 'test' will
;; transform a string like my_table into test_table or  "my_cool_column_name"
;; into test_cool_column_name
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
      (when format
        (insert (format ".mode %s\n" format)))

      (when timer
        (insert (format ".timer %s\n"
                        (if (string= timer "off") "off" "on"))))

      (when headers
        (insert (format ".headers %s\n"
                        (if (string= headers "off") "off" "on"))))

      (when nullvalue
        (insert (format ".nullvalue %s\n" nullvalue)))

      (when separator
        (insert (format ".separator %s\n" separator)))

      (when echo
        (insert (format ".echo %s\n"
                        (if (string= echo "off") "off" "on"))))

      (when bail
        (insert (format ".bail %s\n"
                        (if (string= bail "off") "off" "on"))))

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
Processes raw DuckDB output to remove:
- Version information and connection banners
- Progress bars
- Prompt characters
- Process status messages
- Excess whitespace and blank lines"
  (let ((cleaned-output
         (replace-regexp-in-string
          "\\(?:^D[ ·]+\\|^Process duckdb finished$\\|^Use \"\\.open FILENAME\".*\n\\|^ *[0-9]+% ▕[█ ]+▏.*$\\|^marker$\\|^\\s-*echo:.*\n\\s-*headers:.*\n\\s-*mode:.*\n\\)"
          ""
          output)))
    ;; Trim excess blank lines at the beginning and end
    (string-trim cleaned-output)))

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

(defun org-babel-duckdb-table-or-scalar (result)
  "Convert RESULT into an appropriate Elisp value.
This function analyzes the query result and decides how to format it:

If RESULT appears to be a table (has a separator line after header),
convert it into an Elisp table structure suitable for Org mode.
Otherwise return the string unchanged.

This heuristic approach adapts the return value based on content."
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
Returns the cleaned output of the execution."
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
        ;; Ensure clean prompt state
        (when (> start-point (line-beginning-position))
          (process-send-string process "\n")
          (accept-process-output process 0.01))

        ;; Start fresh
        (goto-char (point-max))
        (setq start-point (point))

        ;; Send command with completion marker
        (process-send-string process
                            (concat full-body
                                    "\n.print \"" completion-marker "\"\n"))

        ;; Wait for output completion with progressive strategy
        (let ((found nil)
              (timeout 60)    ;; 1 minute timeout
              (wait-time 0.05)
              (total-waited 0))

          ;; Keep waiting until we find the completion marker
          (while (and (not found) (< total-waited timeout))
            ;; Wait for process output
            (accept-process-output process wait-time)
            (setq total-waited (+ total-waited wait-time))

            ;; Check if marker is in the buffer
            (save-excursion
              (goto-char start-point)
              (when (search-forward completion-marker nil t)
                (setq found t)))

            ;; Progressive waiting strategy
            (cond
             ((< total-waited 1) (setq wait-time 0.05))  ;; First second: check frequently
             ((< total-waited 5) (setq wait-time 0.2))   ;; Next few seconds: medium checks
             (t (setq wait-time 0.5))))                 ;; After 5 seconds: longer waits

          ;; Process the output
          (if found
              (let* ((marker-pos (save-excursion
                                 (goto-char start-point)
                                 (search-forward completion-marker nil t)))
                     (output-end (when marker-pos (line-beginning-position 0)))
                     (output (buffer-substring-no-properties
                             start-point output-end)))
                ;; Clean up and return the output
                (org-babel-duckdb-clean-output output))
            ;; Handle timeout
            (error "DuckDB query timed out after %d seconds" timeout)))))))
;;; Session Management Functions

(defun org-babel-duckdb-list-sessions ()
  "List all active DuckDB sessions.
Returns an alist of (session-name . buffer) pairs."
  (let (sessions)
    (maphash (lambda (name buffer)
               (push (cons name buffer) sessions))
             org-babel-duckdb-sessions)
    (nreverse sessions)))

(defun org-babel-duckdb-create-session (session-name &optional db-file)
  "Create a new DuckDB session named SESSION-NAME.
If DB-FILE is provided, connect to that database file.
Returns the session buffer."
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

The function handles various execution modes:
- Session vs. non-session execution
- Variable substitution and expansion
- Formatting based on result parameters
- Output buffer display options

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
      (let ((dot-commands (org-babel-duckdb-process-params params)))
        (when dot-commands
          (insert dot-commands "\n"))
        (insert expanded-body)
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

    ;; Handle output to buffer if requested
    (when use-buffer-output
      (org-babel-duckdb-display-buffer raw-result))

    ;; Process the results according to params
    (if use-buffer-output
        ;; If output is directed to a buffer, still return simple results
        (if (member "value" result-params)
            "Output sent to buffer."
          (org-babel-duckdb-table-or-scalar "Output sent to buffer."))

      ;; Normal results processing
      (org-babel-result-cond result-params
        raw-result  ;; Raw string
        (org-babel-duckdb-table-or-scalar raw-result))))) ;; Parsed

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
