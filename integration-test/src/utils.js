//A utility for pretty printing large objects in
//the terminal. For eg. it can be used like this:
// console.log("MyObject: ", inspectObj(myObj))
//And this will print the full object with nested
//arrays etc. and syntax colors
const inspectObj = (obj) => {
  return require("util").inspect(obj, {
    showHidden: false,
    depth: null,
    colors: true,
  });
};

module.exports.inspectObj = inspectObj;
