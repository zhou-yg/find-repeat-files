const fsP = require('node:fs/promises')
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('crypto')

const duplicateDirName = 'the_duplicates'
const exportDirName = 'the_exports'

const entryDir = path.join(__dirname, './test/')

const exportDir = path.join(__dirname, './test/', exportDirName)
const duplicateDir = path.join(__dirname, './test/', duplicateDirName)

if (!fs.existsSync(exportDir)) {
  fs.mkdirSync(exportDir)
}

/**
 * 如果文件夹的格式 YYYY年MM月DD日， 这个文件夹内的所有文件把它们按照年份分类，忽略文件自身的时间戳属性
 * 如果是单独文件散落的文件就按照文件自身的 birthTimeMs 归属
 * 
 * 最终exports的结构
 * exports / YYYY年MM月DD日 / files
 * 
 * 在归属时，如果在目标文件夹的存在有md5相同的文件，则认为是相同的文件，后来者移动到 the_duplicates
 */

const allFiles = [
  /**
   * dir: boolean
   * path: string
   * birthTimeStr: string like YYYY年MM月DD日
   * md5: string  while dir is false
   */
]

const dirChildrenMd5 = {
  /**
   * key: YYYY年MM月DD日
   * value: md5 array
   */
}


traverse(entryDir).then(async () => {
  console.log(`allFiles: ${allFiles.length}`)
  console.log('allFiles: ', allFiles);

  for (const fileObj of allFiles) {
    const dest = path.join(exportDir, fileObj.birthTimeStr)

    if (!dirChildrenMd5[dest]) {
      dirChildrenMd5[dest] = []
    }

    if (fileObj.dir) {
      await moveAndMergeDir(dest, fileObj)
      dirChildrenMd5[dest] = [...new Set([
        dirChildrenMd5[dest],
        ...fileObj.childrenMD5.map(f => f.md5),
      ])]
    } else {
      const childrenMD5 = dirChildrenMd5[dest]
      if (childrenMD5.includes(fileObj.md5)) {
        // duplicate
        await moveToDest(duplicateDir, fileObj.path)
      } else {
        childrenMD5.push(fileObj.md5)
        await moveToDest(dest, fileObj.path)
      }
    }
  }
})

async function traverse (dir) {
  if (!validate(dir)) {
    return
  }

  const files = await fsP.readdir(dir)
  await Promise.all(files.map(async (file) => {
    const fileOrDir = path.join(dir, file)
    const stat = await fsP.stat(fileOrDir)

    const fileObj = {
      dir: stat.isDirectory(),
      path: fileOrDir,
      md5: null,
      childrenMD5: [],
    }

    if (fileObj.dir) {
      if (isDateDir(file)) {
        // 如果是日期文件夹，则直接记录 
        fileObj.birthTimeStr = file
        fileObj.childrenMD5 = await getChildrenMD5(fileOrDir)
        allFiles.push(fileObj)
      } else {        
        await traverse(fileOrDir)
      }
    } else {
      // 如果是文件，整理文件的时间
      const { birthtime } = stat
      fileObj.birthTimeStr = `${birthtime.getFullYear()}年${birthtime.getMonth() + 1}月${birthtime.getDate()}日`

      fileObj.md5 = await getMD5(fileOrDir)
      allFiles.push(fileObj)
    }
  }))
}

// compute md5
function getMD5 (file) {
  return new Promise(resolve => {
    const md5 = crypto.createHash('md5')
    const readStream = fs.createReadStream(file)
    readStream.on('data', (chunk) => {
      md5.update(chunk)
    })
    readStream.on('end', () => {
      const str = md5.digest('hex')
      resolve(str)
    })
  })
}

async function getChildrenMD5 (dir) {
  const files = await fsP.readdir(dir)
  return (await Promise.all(files.map(async (f) => {
    const fp = path.join(dir, f)
    if ((await fsP.stat(fp)).isDirectory()) {
      return {
        dir: true,
        name: f,
        path: fp,
        childrenMD5: await getChildrenMD5(fp)
      }
    } else {
      return {
        dir: false,
        name: f,
        path: fp,
        md5: getMD5(fp)
      }
    }
  }))).filter(Boolean)
}

function validate(dir) {
  return !/the_/.test(dir)
}

function isDateDir(dir) {
  return /^\d+年\d+月\d+日$/.test(dir)
}


async function moveAndMergeDir (destDir, fileObj) {
  const myDir = fileObj.path
  const fileName = path.basename(myDir)

  if (!dirExistMap[destDir]) {
    if (!fs.existsSync(destDir)) {
      await fsP.rename(myDir, destDir)
      dirChildrenMd5[destDir] = fileObj.childrenMD5.map(f => f.md5)
      dirExistMap[destDir] = true
    } else {
      dirExistMap[destDir] = true
      await merge()
    }
  } else {
    await merge()
  }

  async function merge () {
    if (!dirChildrenMd5[destDir]) {
      dirChildrenMd5[destDir] = []
    }
    // 合并文件夹
    const destDirChildrenMD5 = dirChildrenMd5[destDir]

    for (const child of fileObj.childrenMD5) {
      if (child.dir) {
        await moveAndMergeDir(path.join(destDir, child.name), child)
        destDirChildrenMD5.push(...child.childrenMD5.map(f => f.md5))
      } else {
        console.log('destDir ', destDir, destDirChildrenMD5);
        console.log('child.path: ', child.path, destDirChildrenMD5.includes(child.md5));
        if (destDirChildrenMD5.includes(child.md5)) {
          await moveToDest(duplicateDir, child.path)
        } else {
          destDirChildrenMD5.push(child.md5)
          await moveToDest(destDir, child.path)
        }
      }
    }
  }
}
const dirExistMap = {}
async function moveToDest (destDir, filePath) {
  if (!dirExistMap[destDir]) {
    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir)
    }
    dirExistMap[destDir] = true
  }

  const fileName = path.basename(filePath)
  const { name, ext } = path.parse(fileName)
  let i = 0
  let destFileName = path.join(destDir, fileName)
  while (fs.existsSync(destFileName)) {
    i++
    destFileName = path.join(destDir, `${name}(${i})${ext}`)
  }
  await fsP.rename(filePath, destFileName)  
}
