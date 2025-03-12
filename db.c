#include "db.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>

const size_t ID_SIZE = size_of_attribute(row_t, id);
const size_t USERNAME_SIZE = size_of_attribute(row_t, username);
const size_t EMAIL_SIZE = size_of_attribute(row_t, email);
const size_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const off_t ID_OFFSET = 0;
const off_t USERNAME_OFFSET = ID_OFFSET + (off_t)ID_SIZE;
const off_t EMAIL_OFFSET = USERNAME_OFFSET + (off_t)USERNAME_SIZE;

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

    input_buffer -> input_length = bytes_read - 1;
    input_buffer -> buffer[bytes_read - 1] = 0;

}

table_t* db_open(const char* filename){
    pager_t* pager = pager_open(filename);
    size_t num_rows = pager -> file_length / ROW_SIZE;
    table_t* table = (table_t*)malloc(sizeof(table_t));
    
    table -> num_rows = num_rows;
    table -> pager = pager;
    return table;
}

pager_t* pager_open(const char* filename){
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if(fd == -1){
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    pager_t* pager = (pager_t*)malloc(sizeof(pager_t));
    pager -> file_descriptor = fd;
    pager -> file_length = file_length;
    memset(pager -> pages, 0, sizeof(pager -> pages));

    return pager;
}

void* get_page(pager_t* pager, size_t page_num){
    if(page_num > MAX_TABLE_PAGES){
        exit(EXIT_FAILURE);
    }

    if(pager -> pages[page_num] == NULL){
        void* new_page = malloc(PAGE_SIZE);
        size_t num_pages = pager -> file_length / PAGE_SIZE;

        if(pager -> file_length % PAGE_SIZE != 0){
            num_pages += 1;
        }

        if(page_num <= num_pages){
            lseek(pager -> file_descriptor, PAGE_SIZE * page_num, SEEK_SET);
            ssize_t bytes_read = read(pager -> file_descriptor, new_page, PAGE_SIZE);
            if(bytes_read == -1){
                exit(EXIT_FAILURE);
            }
        }
        pager -> pages[page_num] = new_page;
    }
    return pager -> pages[page_num];
}

void pager_flush(pager_t* pager, size_t page_num, size_t size){
    if(pager -> pages[page_num] == NULL){
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager -> file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if(offset == -1){
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager -> file_descriptor, pager -> pages[page_num], size);
    if(bytes_written == -1){
        exit(EXIT_FAILURE);
    }
}

void db_close(table_t* table){
    pager_t* pager = table -> pager;
    size_t num_pages = table -> num_rows / ROWS_PER_PAGE;

    for(size_t i = 0; i < num_pages ; i++){
        if(pager -> pages[i] == NULL){
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager -> pages[i]);
        pager -> pages[i] = NULL;
    }
    
    size_t additional_rows = table -> num_rows % ROWS_PER_PAGE;
    if(additional_rows > 0){
        size_t page_num = num_pages;
        if(pager -> pages[page_num] != NULL){
            pager_flush(pager, page_num, additional_rows * ROW_SIZE);
            free(pager -> pages[page_num]);
            pager -> pages[page_num] = NULL;
        }
    }

    int res = close(pager -> file_descriptor);
    if(res == -1){
        exit(EXIT_FAILURE);
    }

    for(size_t i = 0; i < MAX_TABLE_PAGES; i++){
        if(pager -> pages[i] != NULL){
            free(pager -> pages[i]);
            pager -> pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

meta_command_status_t execute_meta_command(input_buffer_t* input_buffer, table_t* table){   
    if(strcmp(input_buffer -> buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        db_close(table);
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
    void* page = get_page(table -> pager, page_num);
    off_t row_offset = row_num % ROWS_PER_PAGE;
    off_t byte_offset = ROW_SIZE * row_offset;
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