set title ""
#set key bottom right Right reverse
set key off

set xlabel "Results"
set ylabel "Time [ms]"

#set logscale y

set style fill pattern
set terminal pdf  fsize 14 size 5,4

set output "q7_execution.pdf"

plot 'q7_execution.dat' using 4:3 ti 'Q7' with points ps 1.5

