const path = require('path')
const fs = require('fs')
const crypto = require('crypto')

const targetDir = path.join(__dirname, './test')
const repeatDir = path.join(targetDir, '../the_duplicates')

if (!fs.existsSync(repeatDir)) {
  fs.mkdirSync(repeatDir)
}

const concurrentMAX = 1

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


// compute md5
function getMD5 (file, callback) {
  const md5 = crypto.createHash('md5')
  const readStream = fs.createReadStream(file)
  readStream.on('data', (chunk) => {
    md5.update(chunk)
  })
  readStream.on('end', () => {
    const str = md5.digest('hex')
    callback(str)
  })
}
function moveToDuplicates (file, sortIndex, cb) {
  const fileName = path.basename(file)
  const { name, ext } = path.parse(fileName)
  const newFile = path.join(repeatDir, `${name}-${sortIndex}${ext}`)

  fs.rename(file, newFile, err => {
    if (err) {
      throw err
    }
    cb()
  })
}

const allDirs = []
let allFileCount = 0
let allDuplicatesCount = 0
let allDuplicatesSize = 0
const allMD5Map = {}

let completeFileCount = 0
let st = Date.now()

function traverseDirs (p)  {
  fs.readdir(p, (err, files) => {
    if (err) {
      throw err
    }
    files.forEach(f => {
      const fileOrDir = path.join(p, f)
      fs.lstat(fileOrDir, (err, stats) => {
        if (stats.isDirectory()) {
          allDirs.push(fileOrDir)
          traverseDirs(fileOrDir)
        } else {
          allFileCount++
          scheduleConcurrentTasks(function checkAndMove() {
            return new Promise(resolve => {
              getMD5(fileOrDir, md5 => {
                if (allMD5Map[md5] === undefined) {
                  allMD5Map[md5] = 1
                  resolve()

                } else {
                  allMD5Map[md5]++
                  allDuplicatesCount++
                  allDuplicatesSize += stats.size
                  
                  moveToDuplicates(fileOrDir, allMD5Map[md5], () => {
                    completeFileCount++
                    console.log(`complete ${completeFileCount} files, cost ${(Date.now() - st)/1000} s`)
                    resolve()
                  })
                }  
              })
            }).catch(e => {
              console.log('e: ', fileOrDir, e);
            })
          })  
        }
      })
    })
  })
}

traverseDirs(targetDir)

process.on('exit', () => {
  console.log('allFileCount: ', allFileCount);
  console.log('allDuplicatesCount: ', allDuplicatesCount);
  console.log('allDuplicatesSize: ', `${allDuplicatesSize / 1024 / 1024} MB`);
  console.log(`all cost ${(Date.now() - st)/1000} s`)
})