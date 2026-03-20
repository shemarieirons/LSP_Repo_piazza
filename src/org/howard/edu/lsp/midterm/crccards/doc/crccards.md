# CRC Card Collaboration Explanation

TaskManager works with Task because its main job is to manage a group of Task objects. It needs to store them, look them up, and filter them based on status, which requires direct interaction with Task.

Task does not depend on TaskManager because it is only responsible for holding its own data and updating its own status. It does not need to know how tasks are stored or organized elsewhere. This keeps Task simple and makes it easier to reuse in other contexts.