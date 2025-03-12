attempt to make a SQLite clone from scratch in C     (why? because im a big nerd)

basic structure :-

\> parse the query through a tokenizer and parser to create a "bytecode"

\> a "virtual machine" takes this bytecode and operates on the respective tables

\> tables will be stored as B-Trees for efficient operations

\> Each node in tree will be one page in length. Pages will be 4 kilobyte in size (as in most computer architectures)

\> a pager will be resposible for retrieving pages and reading/writing data at proper offsets in the database file


---------------------------------------------------------------------------------------------------------

todo :-

\> currently only able to execute simple INSERT and SELECT command, need to add more operations.

\> Need to implement a B-Tree type data structure later on
