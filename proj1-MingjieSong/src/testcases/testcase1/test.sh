cp input-0.dat INPUT
cat input-1.dat >> INPUT
cat input-2.dat >> INPUT
cat input-3.dat >> INPUT
cat input-4.dat >> INPUT
cat input-5.dat >> INPUT
cat input-6.dat >> INPUT
cat input-7.dat >> INPUT

cp output-0.dat OUTPUT
cat output-1.dat >> OUTPUT
cat output-2.dat >> OUTPUT
cat output-3.dat >> OUTPUT
cat output-4.dat >> OUTPUT
cat output-5.dat >> OUTPUT
cat output-6.dat >> OUTPUT
cat output-7.dat >> OUTPUT
 

go run sort.go INPUT REF_OUTPUT
diff REF_OUTPUT OUTPUT


 ./gensort input-5.dat "200byte"