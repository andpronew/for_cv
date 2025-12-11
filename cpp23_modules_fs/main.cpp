#include <print>
#include <iostream>
#include <string>

import my_module;

int main()
{
    std::string path;
    std::print("Enter a path: ");
    std::cin >> path;
    my_module::bulk_rename(path);
    
    return 0;
}
