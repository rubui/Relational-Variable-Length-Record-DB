## For students: change this path to the root of your code
CODEROOT = ..

#LDLIBS = -lreadline

#CC = gcc
## If you use OS X, then use CC = g++ , instead of CC = g++-4.8
CC = g++
#CC = g++-4.8
CXX = $(CC)


CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11  # with debugging info and the C++11 feature