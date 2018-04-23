
main.pdf : main.tex sample.bib	
	pdflatex main.tex
	bibtex main.aux
	pdflatex main.tex
	pdflatex main.tex
	evince main.pdf &

clean :
	rm -rf *.aux *.pdf *.log *.bbl *.blg *.out


