README

Notes on the Nanostring GeoMx Upload as part of Aligning Science Across Parkinson’s (ASAP-020529), Michael J. Fox Foundation for Parkinson’s Research (MJFF)

1) Tissue samples were processed concurrently for multiple studies—edwards-pmdbs-geomx-th, vila-pmdbs-geomx-unmasked, and vila-pmdbs-geomx-thlc. As a result, ROIs (both TH+ masked and full ROIs) are intermixed within the same .ini and LabWorksheet.txt files. To support reproducibility and transparency, .ini files from all three studies are included.

2) A Seurat object containing metadata and processed counts is provided within artifacts/geomx_edwards_thmask.rds. 
- Processing steps are documented at https://github.com/zchatt/ASAP-SpatialTranscriptomics/tree/main/geomx/lowlevel
- Additional dataset information is availabe at https://zenodo.org/records/13626107