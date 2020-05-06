# HadoopMRFinalProject
This is my Final Project of INFO 7250 Big Data. 

In my project, I plan to use the datasets from food.com that includes comments and recipes. In the comments part, there are about 350MB size of comments from different users on different recipes, and in the recipes part, there are about 300MB size of recipes details, including the user’s information who post it, the ingredients of a recipe, the description of a recipe and the steps to cook it. 

For the MapReduce analysis, blow shows some of my ideas, which will be edited later.
For the comments:
- Number of users’ comments, Average score a use gives
- About Recipe: Partition by rating.

For the recipes:
- Number of contributors and recipes, number of recipes each contributor posts, number of distinct recipes each contributor mentions
- The most active contributor, the most welcomed recipes, the year/month/day that contributors post most
- The most difficult recipe, the easiest recipe (according to number of steps)
- Top quick-cook recipes, number of recipes from each range of cooking time
- Categorize the recipes according to their tags, e.g. areas, spicy/mild, dairy or dairy free
