#include <dirent.h>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include "omp_map_reduce.h"
using namespace std;
#define REPEAT_INPUT 5 // repeat the same set of input this many times
#define min(a, b) a<b? a : b

extern "C" int run_omp(int fnum, char** files, int nThreads);


int main(int argc, char* argv[]){
    int pid, numproc;
    MPI_Status status;

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
                
                filenames.push_back(input_dir+fname);
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
        //send FNUM files to each process
        while(!filenames.empty()){
            for(int i=1; i<numproc; i++){
                
                int no_files_to_sent = min(FNUM, filenames.size());

                for(int j=0; j<no_files_to_sent; j++){
                    // TODO : handle overflows
                    char fname[FLEN];
                    strcpy(fname, filenames[j].c_str());
                    
                    // do a MPI_Send
                    MPI_Send(fname, FLEN, MPI_CHAR, i, i, MPI_COMM_WORLD);
                    
                }
                // remove the sent files
                filenames.erase(filenames.begin(), filenames.begin()+no_files_to_sent);

                if(filenames.empty()) break; 
            }
        }

        // sedn "all done!" msg to all processes
        string s = "all done!";
        char all_done_msg[FLEN];
        strcpy(all_done_msg, s.c_str());
        for(int i=1; i<numproc; i++){
            MPI_Send(all_done_msg, FLEN, MPI_CHAR, i, i, MPI_COMM_WORLD);
        }
    }
    else{
        
        bool all_done = false;
        string s = "all done!";
        char fname[FLEN];
        vector<string> fnames;
        // poll for filenames
        while(!all_done){
            MPI_Recv(fname, FLEN, MPI_CHAR, 0, pid, MPI_COMM_WORLD, &status);
            
            if(strcmp(s.c_str(), fname) == 0) all_done = true;
            else{
            
                string fstr(fname);
                fnames.push_back(fstr);
            
            }
        }

        // call the OMP routine for mapping
        char** filenames = (char**)malloc(sizeof(char*)*fnames.size());
        
        for(unsigned int i=0; i<fnames.size(); i++){
            
            filenames[i] = (char*)fnames[i].c_str();
        
        }
        
        run_omp((int)fnames.size(), filenames, NTHREADS );
            

        free(filenames);
    
    }


    MPI_Finalize();
    return 0;
}
