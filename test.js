const path = require('path')
const fs = require('fs')


fs.renameSync(
  path.join(__dirname, 'test', 'A/a.js'),
  path.join(__dirname, 'test', 'a.js'),
)