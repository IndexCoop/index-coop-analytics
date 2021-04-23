# Contributing to Index Coop Analytics

Thanks for being here!

This is a set of basic guidelines for contributing to the Index Coop Analytics Working Group. While we are getting up and running, this documentation will be sparse. This is intentional - we are not mature enough yet as a working group to run completely hands off, but the goal is to get as close to a mature, open-source software environment over time. As for now, if you are interested in contributing, please reach out to @jdcook#7279 on Discord or ping the #analytics channel in the Index Coop Discord [server](https://discord.gg/4XBGHBKxdU).

## Index Coop Analytics Workflow

### Repository
In order to contribute, fork this repository and clone to your local machine. Set this repository as the remote upstream. You will then work on your local and push to your remote origin, and create Pull Requests back to this repository. [Here](https://gist.github.com/Chaser324/ce0505fbed06b947d962) are some basic instructions on the Fork & Pull Request Workflow.

The root level contains a folder for each of the core lanes across the Coop - kpis, growth, treasury, product, governance. Each project/Issue should fall under one of these areas, so it would warrant creating a new folder inside that lane’s directory. If it is a task for a specific product (ex: DPI) then a new folder should be created inside the `dpi` folder in `product`. Inside each of the core folders is a `dune-queries` folder - all standalone dune queries related to that lane should live there. 

Regarding Dune, all query files should have the link to the dune query at the top of the file. If the project/Issue is itself a dune dashboard, please append “-dash” to the end of the folder name.


### Issues & Project Board
<ul>
<li>New project ideas and requests are created as Issues and land in the `intake` column of the project board. If it is decided that an Issue should be prioritized it then moves to `scoping`.</li>
<li>In `scoping` more description is added to the Issue. Any additional documentation or details needed to complete the project should be included. The Issue will move to `on deck` until it is ready for active work.</li>
<li>Issues that are actively being worked on should be `in progress`. Once work begins on an Issue, a Pull Request should be opened and set into “draft” mode. This allows for transparency and collaboration throughout the life of the project. All communication regarding the Issue should take place in the Issue itself, or in the Pull Request.</li>
<li>When work on an Issue is completed, it is moved to `in review`. All Issues should spend some time here while the work is reviewed by stakeholders and/or other analysts.</li>
<li>Once an Issue has been reviewed and is live / has been delivered, the Issue is moved to `completed` and closed.</li>
<li>Note: Issues will be kept updated with labels for both the lane the Issue is under, and the stage the Issue is at. (Ex: `product` & `scoping`)
</ul>