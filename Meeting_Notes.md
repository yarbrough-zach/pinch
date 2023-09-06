# Meeting Wed, Aug 23 11:00 AM CT

  ## Attendents:
  - Andre Guimaraes
  - Gaby Gonzalez
  - Zach Yarbrough

  ## PyCBC Triggers
  - Binning + Clustering using mchirp and Gamma2.
  - ~1/3 triggers survived (we though it would be more).
  - Currently runinning slow.

  ## GstLAL Triggers
  - Workflow fixed adding "Other Triggers"
  - Running for all GstLAL Triggers again.

  ## Overall Workflow
  - Goals of Work
    - Increase confidence of candidate alerts (a line in DQR).
    - Understanding how glitches affect pipelines.

  ## Todo
  - Talk to CBC and DetChar to schedule presentation on work.
  
# GstLAL F2F Tue, Sep 5, 11:00 AM JST
  - Questions/Concerns to consider:
   - Do we use the same cut of SNR that Gspy on Omicron?
   - Take better care of "Other", maybe eliminate it entirely.
   - Follow up on the ones in the *danger region* that are in the clean set and shouldn’t be.
   - Deadtime of association windows
   - Remove GWs from sets
   - We should color the dirty plots by system (?)
   - Make sure Sqlite files are what we need
   - Percentage-Bar plots over SVD bins (Done)
   - "Difference" plot between histograms (Done)
   - Number of triggers rung up per glitch (Done)
   - Things that have the biggest overlap with the signal model
    - How do you measure that?
   - Add P(signal|theta)
    - Injections
   - Misclassification of BBH/chirp vs tomte
   - We want to answer the question “is the probability that what we’ve observed is a glitch”
   - If we use SVDbin, SNR, Chisq, we can directly use the signal model (maybe)

