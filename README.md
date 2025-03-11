# Exercise 1 
## compiling
use cmake version >= 3.0 to compile
```
mkdir build
cd build
cmake ..
cmake --build .
```

## testing 
you first need to generate the files to sort
```
exercise01 gen-input <filsize in mb> <input filename> <output filename>
```
then you can sort them with external memory mergesort like this:
```
exercise01 <input filename> <output filename> <input file size in mb>
```
the output will automatically be tested (see the test function in the code)