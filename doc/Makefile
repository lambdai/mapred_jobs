PDF = $(patsubst %.tex,%.pdf, $(wildcard *.tex))
PNG = $(patsubst %.tex,%.png, $(wildcard *.tex))

all: $(PDF) $(PNG)

%.pdf: %.tex
	xelatex $<

%.png: %.pdf
	convert -density 150 -antialias $< -flatten -antialias $@

clean:
	rm -f $(PDF) $(PNG)
