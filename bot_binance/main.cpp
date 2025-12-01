#include <iostream>
#include "logging.h"

using namespace std;

void run_bot();

int main()
{
     init_logger("/home/andpro/Documents/Andr/bot/logs/bot_output.txt");
    try {
        run_bot();
    } catch (const exception& e) {
        cerr << "Exception caught: " << e.what() << endl;
    }
    return 0;
}
