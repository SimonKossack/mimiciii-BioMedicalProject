**EHR-Project**

**Goal:** Prediction of patient disease state and trajectory.

**Background:** MIMIC-III is a large, freely-available database comprising deidentified health-related data associated with over forty thousand patients who stayed in critical care units of the Beth Israel Deaconess Medical Center between 2001 and 2012. The database includes information such as demographics, vital sign measurements made at the bedside (~1 data point per hour), laboratory test results, procedures, medications, caregiver notes, imaging reports, and mortality (including post-hospital discharge). **Please note that you will have to undergo training (approx. 2hrs) before you can get access to the data.**

**Task:**

- **Effects of interventions**
    - Which interventions should we do and at what time?
    - Potential interventions include vasopressors, mechanical ventilation, dialysis, and cardiac assist devices.
    - Can you identify subgroups of patients responding worse to a particular intervention (e.g. males vs. females)?

We expect a report adhering to these broad sections:

- **Introduction**: This section should include a brief explanation of your problem and its clinical importance. You should briefly explain your basic approach and your main conclusions. A figure is often helpful to motivate the work.
- **Related work**: This section should highlight previous work related to your problem and should put your work in a broader context.
- **Methods**: Here you should formally define your problem and describe the method you implemented in detail. Include any simplifying assumptions that you make about your data or the general problem. You should enumerate any modelling choices that you had to make and justify your choices. You are free to choose models (e.g. linear, decision tree, …) of your liking. A main figure illustrating the overall methodology often adds a lot.
- **Data and experiment setup**: Include details about your data, what variables you have access to, your cohort selection criteria, and your preprocessing choices. You might find it useful to include a table with population characteristics, or an example of the data available for a specific individual, both before (i.e. the original data) and after any pre-processing (i.e. feature construction), to make the discussion concrete.
- **Results**: Report the quantitative results of your analyses. You may choose to present graphs or tables, the important thing is that your tables and plots should summarize the relevant results that you got out of the analysis. Comment on these results: are they statistically significant? Are there interesting trends? Is there a significant treatment effect? You may also present qualitative results such as an in-depth analysis of what the approach would do for a few randomly chosen patients. Report meaningful performance metrics.
- **Discussion**: Highlight how your results relate to your original question formulation. Do they reveal interesting insights about existing medical practices, the nature of diseases, etc? Discuss limitations with your analyses and how they might motivate future research directions.