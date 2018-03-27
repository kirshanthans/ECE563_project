#include <dirent.h>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
using namespace std;
#define REPEAT_INPUT 5 // repeat the same set of input this many times

int main(int argc, char* argv[]){
    int pid, numproc;

    MPI_Init(&argc, &argv);                 
    MPI_Comm_size(MPI_COMM_WORLD, &numproc);   
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);  
    
    // every node will ask files from master. If files available
    // master will send '1' follwed by the file name. else send '0' (all done).
    if(pid == 0){
        DIR* dir;
        struct dirent * entry;
        vector<string> filenames;
        string input_dir = "../RawText/";

        if((dir = opendir(input_dir.c_str())) != NULL){
            while((entry = readdir(dir)) != NULL){
                string fname(entry->d_name);
                //cout << fname << endl;
                if(fname == "." || fname == "..") continue;
                
                filenames.push_back(fname);
            }
            closedir(dir);
        }
        else{
            // error
            return -1;
        }
        
        // Now to increse the size of input reuse the same input files 
        unsigned int no_of_files = filenames.size();
        //cout << no_of_files << endl;
        for(int i=0; i<REPEAT_INPUT-1; i++){
            for(unsigned int j=0; j<no_of_files; j++) filenames.push_back(filenames[j]);
        }
        //cout << filenames.size() << endl;
        
    }
    else{
    
    }


    MPI_Finalize();
    return 0;
}
