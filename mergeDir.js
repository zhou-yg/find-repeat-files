const path = require('path')
const fs = require('fs')
const crypto = require('crypto')

const destDirName = 'the_merges'

const targetDir = path.join(__dirname, './test')
const destDir = path.join(targetDir, `./${destDirName}`)

if (!fs.existsSync(destDir)) {
  fs.mkdirSync(destDir)
}


const allDirs = fs.readdirSync(targetDir).filter(f => {
  return !/^the_/.test(f)
}).map(f => path.join(targetDir, f))


allDirs.forEach(dir => {
  traverseDir([], dir)
})

const concurrentMAX = 300

let waitQueue = []

let runPromise = null

function runTask () {
  const maxQ = waitQueue.slice(0, concurrentMAX)
  if (maxQ.length) {
    waitQueue = waitQueue.slice(concurrentMAX)
    const pArr = maxQ.map(f => f())
  
    Promise.all(pArr).then(() => {
      runTask()
    })
  } else {
    runPromise = null 
  }  
}

function scheduleConcurrentTasks(fn) {
  waitQueue.push(fn)
  if (runPromise) {
    return
  } else {
    runPromise = Promise.resolve().then(() => {
      runTask()
    })
  }
}

function traverseDir (parent, dir) {
  fs.readdir(dir, (err, files) => {
    if (err) {
      throw err
    }

    if (files.length > 0 && parent.length > 0) {
      const sub = path.join(destDir, ...parent)
      if (!fs.existsSync(sub)) {
        fs.mkdirSync(sub)
      }
    }
    files.forEach(f => {
      const fileOrDir = path.join(dir, f)
      fs.lstat(fileOrDir, (err, stat) => {
        if (err) {
          throw err
        }
        if (stat.isDirectory()) {
          traverseDir(
            parent.concat(f),
            fileOrDir
          )
        } else {
          let destFile = path.join(destDir, ...parent, f)
          scheduleConcurrentTasks(() => {
            return new Promise((resolve, reject) => {
              let i = 2;
              console.log('fs.existsSync(destFile): ', fs.existsSync(destFile), fileOrDir, destFile);
              while (fs.existsSync(destFile)) {
                const { name, ext } = path.parse(f)
                destFile = path.join(destDir, ...parent, `${name}-${i++}${ext}`)
              }
              const rs = fs.createReadStream(fileOrDir)
              const ws = fs.createWriteStream(destFile)
              rs.pipe(ws)
              ws.on('finish', () => {
                resolve()
              })
              ws.on('error', () => {
                reject()
              })
            })
          })
        }
      })
    })
  })

}