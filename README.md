# Pipeline-Informed Noise CHaracterization (PINCH) 

[![paper](https://img.shields.io/badge/arXiv-2505.14949-blue)](https://arxiv.org/abs/2505.14949)

PINCH is a framework for identifying, categorizing, and mitigating glitch-induced triggers in matched-filter searches for compact binary coalescences (CBCs). It implements the methodology described in our paper:

> *PINCH: Pipeline-Informed Noise Characterization in LIGO's Third Observinr Run*  
> Zach Yarbrough *et al* 2025 *Class. Quantum Grav.* 42 165014

---

## Motivation

Gravitational-wave detectors are plagued by short-duration, non-astrophysical noise transients (“glitches”). Search pipelines such as GstLAL produce thousands of triggers that may be due to either astrophysical signals or glitches. PINCH leverages information from the pipeline itself to:

- Learn about the LIGO noise populations from the perspective of the search pipeline.
- Identify when search triggers are strongly correlated with known glitch classes.
- Quantify how different glitch types populate search parameter space.
- Provide insights to possibly improve robustness of CBC candidate selection.

---

## Approach

1. **Overlap Pipeline**  
   - Compares search triggers to auxiliary glitch catalogs (Gravity Spy, Omicron).  
   - Separates triggers into clean (no glitch overlaps) and dirty (glitch overlaps) sets.

2. **SVM Pipeline**  
   - Trains Support Vector Machine classifiers on clean/dirty data.  
   - Scores ambiguous triggers, providing a quantitative “glitch-likeness” metric.

3. **Outputs**  
   - CSVs of classified triggers per interferometer (IFO).  
   - Optional saved SVM models for reuse.  
   - Diagnostic plots and statistics.

---

### Results

The PINCH method shows that some glitch classes consistently populate narrow regions of search parameter space, while others are more diffuse.
#Evolution of glitch impacts over time
#Parameter-space clustering for specific glitch types
#Performance of the SVM classifier on O3 data

---

## Installation

Clone and install via `pip` in a fresh environment:

```bash
git clone git@github.com:yarbrough-zach/pinch.git
cd pinch
pip install -e .
```

---

## Usage

A command line interface is provided

```
overlap-and-svm \
  --ifos H1 L1 \
  --pipeline-triggers /path/to/triggers.csv \ # or duckdb
  --output-dir results/overlap \
  --gspy \
  --omicron \
  --omicron-paths H1:/path/H1.csv,L1:/path/L1.csv \ # or duckdb
  --scored-output-path results/scored
```

Key options:
--gspy: enable Gravity Spy overlap
--omicron: enable Omicron overlap
--omicron-paths: map IFOs to Omicron CSVs
--score-only: skip training, score only
Outputs are written to the given output directory.

## Citation

If you use this code or methodology in your work, please cite:

```bibtex
@article{Yarbrough:2025tzn,
  author        = "Yarbrough, Zach and Guimaraes, Andre and Joshi, Prathamesh and Gonz{\'a}lez, Gabriela and Valentini, Andrew",
  title         = "{PINCH: Pipeline-Informed Noise Characterization of Gravitational Wave Candidates}",
  eprint        = "2505.14949",
  archivePrefix = "arXiv",
  primaryClass  = "gr-qc",
  month         = "5",
  year          = "2025"
}
```
