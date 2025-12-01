#pragma once
#include <string>

// Initialize logger; default path if empty => ./bot_output.txt
void init_logger(const std::string &path = "./bot_output.txt");

// Log a plain text message (INFO)
void log_message(const std::string &msg);

// Log an order response (raw JSON string). Function will parse JSON and write structured line + RAW.
void log_order_response(const std::string &response_json);

