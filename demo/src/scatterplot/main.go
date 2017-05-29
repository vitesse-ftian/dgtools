package main

import (
	"encoding/csv"
	"flag"
	"image"
	"image/color"
	"image/png"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/gonum/plot"
	"github.com/gonum/plot/plotter"
	"github.com/gonum/plot/vg"
	"github.com/gonum/plot/vg/draw"
	"github.com/gonum/plot/vg/vgimg"
)

func parseColor(c string) color.RGBA {
	switch c {
	case "red":
		return color.RGBA{R: 255, A: 255}
	case "green":
		return color.RGBA{G: 255, A: 255}
	case "blue":
		return color.RGBA{B: 255, A: 255}
	case "black":
		return color.RGBA{A: 255}
	default:
		log.Panicf("Unknown color %s", c)
		return color.RGBA{}
	}
}

func parseGlyph(g string) draw.GlyphDrawer {
	switch g {
	case "box":
		return draw.BoxGlyph{}
	case "circle":
		return draw.CircleGlyph{}
	case "cross":
		return draw.CrossGlyph{}
	case "pyramid":
		return draw.PyramidGlyph{}
	case "ring":
		return draw.RingGlyph{}
	case "square":
		return draw.SquareGlyph{}
	case "triangle":
		return draw.TriangleGlyph{}
	default:
		log.Panicf("Unknown glyph: %s", g)
		return nil
	}
}

func main() {
	ofile := flag.String("o", "", "Output file name")
	flag.Parse()

	r := csv.NewReader(os.Stdin)
	r.Comma = '|'

	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Error reading input.")
	}

	series := make(map[string][]float64)

	for _, rec := range records {
		marker := strings.TrimSpace(rec[0])
		x, err := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
		if err != nil {
			log.Fatalf("Data error. x, %s", rec[1])
		}
		y, err := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
		if err != nil {
			log.Fatalf("Data error. y, %s", rec[2])
		}

		ss := series[marker]
		series[marker] = append(ss, x, y)
	}

	p, err := plot.New()
	if err != nil {
		log.Panicf("Cannot open plot.")
	}
	// p.Title.Text = "ScatterPlot"
	p.X.Label.Text = "X"
	p.Y.Label.Text = "Y"
	p.Add(plotter.NewGrid())

	for k, v := range series {
		cg := strings.Split(k, "-")
		c := parseColor(cg[0])
		g := parseGlyph(cg[1])
		r := 5.0
		if len(cg) == 3 {
			r, err = strconv.ParseFloat(cg[2], 64)
			if err != nil {
				log.Panicf("Bad marker %s", k)
			}
		}

		pts := make(plotter.XYs, len(v)/2)
		for i := 0; i < len(v); i += 2 {
			pts[i/2].X = v[i]
			pts[i/2].Y = v[i+1]
		}
		s, err := plotter.NewScatter(pts)
		if err != nil {
			log.Panicf("Cannot create scatter series %s", k)
		}
		s.GlyphStyle.Color = c
		s.GlyphStyle.Shape = g
		s.GlyphStyle.Radius = vg.Length(r)
		p.Add(s)
	}

	if *ofile == "" {
		img := image.NewRGBA(image.Rect(0, 0, 1024, 1024))
		c := vgimg.NewWith(vgimg.UseImage(img))
		p.Draw(draw.New(c))
		err = png.Encode(os.Stdout, img)
		if err != nil {
			log.Panicf("Cannot draw to canvas, err %s", err.Error())
		}
		// feh?
	} else {
		err = p.Save(4*vg.Inch, 4*vg.Inch, *ofile)
		if err != nil {
			log.Panicf("Cannot save to file %s", *ofile)
		}
	}
}
