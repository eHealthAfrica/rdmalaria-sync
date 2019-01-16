let handler = require('./handler')

handler.sync(null, null, (_, msg) => console.log(msg))
