#pragma once

#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#define MAX_TABLE_PAGES 100

typedef struct{
    int file_descriptor;
    size_t file_length;
    void* pages[MAX_TABLE_PAGES];
} pager_t;

typedef struct{
    size_t row_num;
    table_t* table;
    bool end_of_table;
} cursor_t;

typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED,
} meta_command_status_t;

typedef enum{
    PREPARE_STATEMENT_SUCCESS,
    PREPARE_STATEMENT_UNRECOGNIZED,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
} prepare_status_t;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
} execute_status_t;

typedef enum{
    SELECT,
    INSERT,
} statement_type_t;

typedef struct {
    uint32_t id;
    char username[33];
    char email[256];
} row_t;

typedef struct{
    size_t num_rows;
    pager_t* pager;
} table_t;

typedef struct{
    statement_type_t type;
    row_t row_to_insert;
} statement_t;

#define size_of_attribute(Struct, attribute) sizeof(((Struct*)0) -> attribute)

extern const size_t ID_SIZE;
extern const size_t USERNAME_SIZE;
extern const size_t EMAIL_SIZE;
extern const size_t ROW_SIZE;
extern const off_t ID_OFFSET;
extern const off_t USERNAME_OFFSET;
extern const off_t EMAIL_OFFSET;
extern const size_t PAGE_SIZE;
extern const size_t ROWS_PER_PAGE;
extern const size_t MAX_ROWS_PER_TABLE;

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} input_buffer_t;

void print_db_prompt();

input_buffer_t* allocate_input_buffer();
void read_input_to_buffer(input_buffer_t* input_buffer);
void close_input_buffer(input_buffer_t* input_buffer);

meta_command_status_t execute_meta_command(input_buffer_t* input_buffer, table_t* table);
prepare_status_t prepare_insert(input_buffer_t* input_buffer, statement_t* statement);
prepare_status_t prepare_statement(input_buffer_t* input_buffer, statement_t* statement);

void serialize_row(void* destination, row_t* source);
void deserialize_row(row_t* destination, void* source);
void print_row(row_t* row);
void* cursor_loc(cursor_t* cursor);
void cursor_increment(cursor_t* cursor);

table_t* db_open(const char* filename);
pager_t* pager_open(const char* filename);
void* get_page(pager_t* pager, size_t page_num);
void db_close(table_t* table);
void pager_flush(pager_t* pager, size_t page_num, size_t size);

cursor_t* table_start_cursor(table_t* table);
cursor_t* table_end_cursor(table_t* table);

execute_status_t execute_select(statement_t* statement, table_t* table);
execute_status_t execute_insert(statement_t* statement, table_t* table);
execute_status_t execute_statement(statement_t* statement, table_t* table);