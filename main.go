package main

//so what are we trying to build here .. First a simple server that takes in a post request containing the "issue" that we are
//working with there ..
//request comes in via the INPUT NODE
// -> AGENT/DECISION NODE for classification
// -> TOOL CALL NODE to get information about the user.
// -> (CONDITION/BRANCH NODE) Choose Execution path based on the classification + User Info
// (This might be flagged as unclear and be set to humans for approval which will freeze it on HUMAN APPROVAL NODE)
// -> Give the final response back TERMINAL NODE.
