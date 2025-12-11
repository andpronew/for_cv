module;

#include <print>
#include <iostream>
#include <string>
#include <filesystem>

export module my_module;

export namespace my_module
{
    void bulk_rename(const std::string& path_str)
    {
        namespace fs = std::filesystem;
        const fs::path dir =path_str;
        for (const auto &file : fs::directory_iterator(dir))
        {
            if(!file.is_regular_file()) 
            {
                continue;
            }
            const fs::path file_path = file.path();
            const fs::path parent = file_path.parent_path();
            const fs::path stem = file_path.stem();
            const fs::path ext = file_path.extension();
            
            
            const std::string new_filename = stem.string() + "_hi_Mike_" + ext.string(); 
            const fs::path new_file_path = parent / new_filename;
            fs::rename(file_path, new_file_path);

        }
    }
}