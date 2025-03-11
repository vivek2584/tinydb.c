#include "db.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

const size_t ID_SIZE = size_of_attribute(row_t, id);
const size_t USERNAME_SIZE = size_of_attribute(row_t, username);
const size_t EMAIL_SIZE = size_of_attribute(row_t, email);
const size_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const size_t ID_OFFSET = 0;
const size_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const size_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const size_t PAGE_SIZE = 4096;             // 4KB
const size_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const size_t MAX_ROWS_PER_TABLE = MAX_TABLE_PAGES * ROWS_PER_PAGE;

void print_db_prompt(){
    printf("db > ");
}

input_buffer_t* allocate_input_buffer(){
    input_buffer_t* new_buffer = (input_buffer_t*)malloc(sizeof(input_buffer_t));
    new_buffer -> buffer = NULL;
    new_buffer -> buffer_length = 0;
    new_buffer -> input_length = 0;
    return new_buffer;
}

void close_input_buffer(input_buffer_t* input_buffer){
    free(input_buffer -> buffer);
    free(input_buffer);
}

void read_input_to_buffer(input_buffer_t* input_buffer){
    ssize_t bytes_read = getline(&(input_buffer -> buffer), &(input_buffer -> buffer_length), stdin);
    
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;

}

table_t* allocate_table(){
    table_t* table = (table_t*)malloc(sizeof(table_t));
    table -> num_rows = 0;
    memset(table -> pages, 0 , sizeof(table -> pages));
    return table;
}

void free_table(table_t* table){
    for(size_t i = 0; i < MAX_TABLE_PAGES; i++){
        free(table -> pages[i]);
    }
    free(table);
}

meta_command_status_t execute_meta_command(input_buffer_t* input_buffer){   
    if(strcmp(input_buffer -> buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
    else{
        return META_COMMAND_UNRECOGNIZED;
    }
}

prepare_status_t prepare_insert(input_buffer_t* input_buffer, statement_t* statement){
    statement -> type = INSERT;

    char* keyword = strtok(input_buffer -> buffer, " ");
    char* string_id = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if(string_id == NULL || username == NULL || email == NULL)
        return PREPARE_SYNTAX_ERROR;

    int id = atoi(string_id);
    
    if(id < 0)
        return PREPARE_NEGATIVE_ID;
    if(strlen(username) > USERNAME_SIZE)
        return PREPARE_STRING_TOO_LONG;
    if(strlen(email) > EMAIL_SIZE)
        return PREPARE_STRING_TOO_LONG;

    statement -> row_to_insert.id = id;
    strcpy(statement -> row_to_insert.username, username);
    strcpy(statement -> row_to_insert.email, email);

    return PREPARE_STATEMENT_SUCCESS;
}

prepare_status_t prepare_statement(input_buffer_t* input_buffer, statement_t* statement){
    if(strncmp(input_buffer -> buffer, "insert", 6) == 0){
        return prepare_insert(input_buffer, statement);
    }
    if(strcmp(input_buffer -> buffer, "select") == 0){
        statement -> type = SELECT;
        return PREPARE_STATEMENT_SUCCESS;
    }
    return PREPARE_STATEMENT_UNRECOGNIZED;
}

void serialize_row(void* destination, row_t* source){
    memcpy(destination + ID_OFFSET, &(source -> id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, source -> username, USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, source -> email, EMAIL_SIZE);
}

void deserialize_row(row_t* destination, void* source){
    memcpy(&(destination -> id), source + ID_OFFSET, ID_SIZE);
    memcpy(destination -> username, source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(destination -> email, source + EMAIL_OFFSET, EMAIL_SIZE);
}

void print_row(row_t* row){
    printf("(%d %s %s)\n", row -> id, row -> username, row -> email);
}

void* find_row_location(table_t* table, size_t row_num){
    size_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table -> pages[page_num];
    if(page == NULL){
        page = malloc(PAGE_SIZE);
        table -> pages[page_num] = page;
    }
    size_t row_offset = row_num % ROWS_PER_PAGE;
    size_t byte_offset = ROW_SIZE * row_offset;
    return page + byte_offset;
}

execute_status_t execute_insert(statement_t* statement, table_t* table){
    if(table -> num_rows >= MAX_ROWS_PER_TABLE){
        return EXECUTE_TABLE_FULL;
    }
    row_t* row = &(statement -> row_to_insert);
    serialize_row(find_row_location(table, table -> num_rows), row);
    table -> num_rows += 1;

    return EXECUTE_SUCCESS;
}

execute_status_t execute_select(statement_t* statement, table_t* table){
    row_t row;
    for(size_t i = 0; i < table -> num_rows; i++){
        deserialize_row(&row, find_row_location(table, i));
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

execute_status_t execute_statement(statement_t* statement, table_t* table){
    switch(statement -> type){
        case SELECT:
            return execute_select(statement, table);
        case INSERT:
            return execute_insert(statement, table);
    }
}