#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>

#include "db.h"

int main(int argc, char** argv){
    input_buffer_t* input_buffer = allocate_input_buffer();
    table_t* table = allocate_table();
    while(true){
        print_db_prompt();
        read_input_to_buffer(input_buffer);
        if (input_buffer->buffer[0] == '.'){
            switch(execute_meta_command(input_buffer)){
                case META_COMMAND_SUCCESS:                      // not yet returned by the execute_meta_command function
                    continue;
                case META_COMMAND_UNRECOGNIZED:
                    printf("Error: Unrecognized Command : %s\n", input_buffer -> buffer);
                    continue;
            }
        }

        statement_t statement;
        switch(prepare_statement(input_buffer, &statement)){
            case PREPARE_STATEMENT_SUCCESS:
                break;
            case PREPARE_STATEMENT_UNRECOGNIZED:
                printf("Error: Unrecognized keyword at start of %s\n", input_buffer -> buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Error: Syntax Error. Could not parse Statement.\n");
        }

        switch(execute_statement((&statement), table)){
            case EXECUTE_SUCCESS:
                printf("Executed\n");
                continue;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table FULL\n");
                break;
        }
    }
}