This Zenodo deposit contains a publicly available description of the Dataset:

# "Deep bulk RNAseq of neurological controls and PD brains".

## Dataset Description:
 
Human post-mortem brain tissue from donors with Lewy Body pathology and neurologically healthy controls was sourced from the Cambridge Brain Bank under London–Bloomsbury Research Ethics Committee (REC reference no. 16/LO/0508). Donors with Lewy Body pathology (n=25) had a clinical diagnosis of Parkinson's disease and were compared to an age- and sex-matched group of neurologically healthy control donors (n=18) who had no history of neurological illness. A neuropathologist assessed all cases for Lewy Body pathology Braak staging, co-existing proteinopathies and other pathologies. Fresh frozen tissue was sampled from SN,  PUT, AMY, and PFC at the level of Brodmann area 46 (PFC). A 3 mm3 tissue piece was cut from the same tissue block that had been used for snRNA sequencing and disrupted using a Tissuelyser (Qiagen). A steel bead and RLT buffer with -mercaptoethanol was added to the Tissuelyser and shaken at 30 Hz for 2 minutes. Total RNA was isolated from the disrupted tissue using the RNeasy Mini Kit (Qiagen; RRID). The sequencing libraries were then generated using Illumina TruSeq Stranded mRNA library prep kit (with poly-A selection) and sequenced on a NovaSeq6000 or Novaseq X plus (2 x 150 paired end). Basecalling and sample-specific fastq files were done using Illumina's bcl2fastq  in default parameters. Reads were uniquely mapped to hg38 reference genome using STAR aligner (version 2.7.8a; REF; RFID, --outFilterMultimapNmax 1, --outFilterMismatchNoverLmax 0.03).  TE and gene quantification was performed using featureCounts (subread package version 1.6.3; REF; RFID -s 2) and gencode annotation version 38, repeatmasker (open-4.0.5, filtered from tRNAs, simple repeats, small RNAs, and low-complexity regions), or retrotector predictions annotation.


**Authors:**

* Jakobsson, Johan; [ORCID:0000-0003-0669-7673](0000-0003-0669-7673); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.
* Barker, Roger; [ORCID:0000-0001-8843-7730](0000-0001-8843-7730); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Gale Hammell, Molly; [ORCID:0000-0003-0405-8392](0000-0003-0405-8392); Institute for Systems Genetics, Department of Neuroscience and Physiology, NYU Langone Health, New York, NY 10016, USA. 
Neuroscience Institute, NYU Grossman School of Medicine, New York, NY 10016, USA.
* Kirkeby, Agnete; [ORCID:0000-0001-8203-6901](0000-0001-8203-6901); Novo Nordisk Foundation Center for Stem Cell Medicine (reNEW) and Department of Neuroscience, University of Copenhagen, Copenhagen, Denmark.
* Garza, Raquel; [ORCID:0000-0002-2524-3055](0000-0002-2524-3055); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.
* Sharma, Yogita; [ORCID:0000-0001-9702-1809](0000-0001-9702-1809); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.
* Tam, Oliver; [ORCID:0000-0002-1023-3655](0000-0002-1023-3655); Institute for Systems Genetics, Department of Neuroscience and Physiology, NYU Langone Health, New York, NY 10016, USA. 
Neuroscience Institute, NYU Grossman School of Medicine, New York, NY 10016, USA.
* Wunderlich, Cole; [ORCID:0000-0002-6171-888X](0000-0002-6171-888X); Institute for Systems Genetics, Department of Neuroscience and Physiology, NYU Langone Health, New York, NY 10016, USA. 
Neuroscience Institute, NYU Grossman School of Medicine, New York, NY 10016, USA.
* Forcier, Talitha; [ORCID:0000-0002-7933-5086](0000-0002-7933-5086); Institute for Systems Genetics, Department of Neuroscience and Physiology, NYU Langone Health, New York, NY 10016, USA. 
Neuroscience Institute, NYU Grossman School of Medicine, New York, NY 10016, USA.
* Jones, Jo; [ORCID:0000-0003-4974-1371](0000-0003-4974-1371); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Curle, Annabel; [ORCID:0000-0002-9105-1131](0000-0002-9105-1131); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Quaegebeur, Annelies; [ORCID:0000-0001-5357-9341](0000-0001-5357-9341); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Adami, Anita; [ORCID:0000-0002-9421-7942](0000-0002-9421-7942); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.
* Wijesinghe, Sasvi; [ORCID:0009-0004-6811-0043](0009-0004-6811-0043); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Jönsson, Marie; [ORCID:0000-0002-1184-6269](0000-0002-1184-6269); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.
* Kouli, Antonina; [ORCID:0000-0001-6553-6154](0000-0001-6553-6154); Department of Clinical Neurosciences, University of Cambridge and Department of Pathology, Cambridge University Hospitals NHS Foundation Trust, Cambridge, UK
* Atacho, Diahann; [ORCID:0000-0002-6158-0235](0000-0002-6158-0235); Laboratory of Molecular Neurogenetics, Department of Experimental Medical Science, Wallenberg Neuroscience Center and Lund Stem Cell Center, BMC A11, Lund University, 221 84 Lund, Sweden.


**ASAP Team:** Jakobsson

**Dataset Name:** jakobsson-pmdbs-bulk-rnaseq, v1.0

**Principal Investigator:** Johan Jakobsson, johan.jakobsson@med.lu.se

**Dataset Submitter:** Raquel Garza, raquel.garza@med.lu.se

**Publication DOI:** No publication DOI exists at the moment

**Grant IDs:** ['ASAP-000520']

**ASAP Lab:** Activation of transposable elements as a trigger of neuroinflammation in Parkinson's disease

**ASAP Project:** Activation of transposable elements as a trigger of neuroinflammation in Parkinson's disease

**Project Description:** A range of genetic  clinical and pathological studies have established that inflammation is a central component of neurodegenerative disorders including Parkinson's disease (PD). However what still remains unclear is the underlying cause for this inflammation. Here we propose a novel hypothesis involving the aberrant activation of transposable elements (TEs) as the trigger for neuroinflammation in PD. TEs are viral-like mobile genetic elements that comprise nearly 50% of the human genome. We propose that the combination of age and the underlying pathogenic processes in PD result in their aberrant activation and with this the expression of viral-like sequences in the brain which in turn activates an immune response (as would be the case for any viral infection in the brain) and then drives further neurodegeneration. This theory by invoking an entirely new pathogenic mechanism will open up a number of new avenues of PD-research with clear clinical relevance. This includes novel diagnostic biomarkers that could be developed based on the presence of TE-derived transcripts or peptides as well as new drug targets that could be exploited for example by targeting viral-like mechanisms or viral-induced inflammation. In summary this project has the potential to identify a novel disease-contributing mechanism in PD with a high clinical potential for development of much needed diagnostics and therapies for this condition.

**Submission Date:** 2025-08-31

__________________________________________


> This dataset is made available to researchers via the ASAP CRN Cloud: [cloud.parkinsonsroadmap.org](https://cloud.parkinsonsroadmap.org). Instructions for how to request access can be found in the [User Manual](https://storage.googleapis.com/asap-public-assets/wayfinding/ASAP-CRN-Cloud-User-Manual.pdf).

> This research was funded by the Aligning Science Across Parkinson's Collaborative Research Network (ASAP CRN), through the Michael J. Fox Foundation for Parkinson's Research (MJFF).

> This Zenodo deposit was created by the ASAP CRN Cloud staff on behalf of the dataset authors. It provides a citable reference for a CRN Cloud Dataset

