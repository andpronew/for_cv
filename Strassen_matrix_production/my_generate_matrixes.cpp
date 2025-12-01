#include <iostream>
#include <fstream>
#include <random>

int matrix_generator(char M, int N, std::ofstream &fs_M)
{
    // random number generator
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(1, 9);

    std::cout << "Generating matrix " << M << "...\n";
    for (int i{}; i != N; ++i)
    {
        for (int j{}; j != N; ++j)
        {
            fs_M << dist(rng) << "\n";
        }
    }
    return 0;
}

int main()
{
    int N{};
    std::cout << "Enter matrix dimension N: ";
    std::cin >> N;
    //std::ofstream fs;

    // open file
    std::ofstream fs_A("matrix_input.txt");
    matrix_generator('A', N, fs_A);
    fs_A.close();
    std::ofstream fs ("matrix_input.txt", std::ios::app);
    matrix_generator('B', N, fs);

  
    fs.close();

    std::cout << "matrix_input.txt is ready.\n";
    return 0;
}

/*Simple generator:

int main()
{
    int N = 2; // matrix dimension

    std::ofstream fs("matrix_input.txt");

    // A matrix
    fs << 1 << "\n";
    fs << 2 << "\n";
    fs << 3 << "\n";
    fs << 4 << "\n";

    // B matrix
    fs << 5 << "\n";
    fs << 6 << "\n";
    fs << 7 << "\n";
    fs << 8 << "\n";

    fs.close();

    return 0;
}
*/



/* if (!fs.is_open())
   {
       std::cout << "Error: cannot create file.\n"
                 << std::endl;
       return 1;
   }*/
/*    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(-5, 5);

    // Fill first matrix A
    std::cout << "Generating matrix A...\n" << std::endl;
    for (int i{}; i != N; ++i)
    {
        for (int j{}; j != N; ++j)
        {
            fs << dist(rng) << "\n";
        }
    }
    // Fill second matrix B
    std::cout << "Generating matrix B...\n" << std::endl;
    for (int i{}; i != N; ++i)
    {
        for (int j{}; j != N; ++j)
        {
            fs << dist(rng) << "\n";
        }
    }
*/
