names := lab0 lab1 lab2 lab3 lab4 lab5
names += lesson

pdfs := $(addsuffix .pdf,$(names))

all: ${pdfs}

%.pdf: %.tex $(shell find include -name '*.tex' -o -name '*.bib')
	latexmk -silent -pdf -pvc- $<

clean:
	rm -f *-blx.bib *.aux *.bbl *.blg *.fdb_latexmk *.fls *.log *.nav *.out *.snm *.toc *.xml

.PHONY: all clean
