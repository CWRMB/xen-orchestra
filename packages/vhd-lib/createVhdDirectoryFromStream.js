'use strict'

const { createLogger } = require('@xen-orchestra/log')
const { parseVhdStream } = require('./parseVhdStream.js')
const { VhdDirectory } = require('./Vhd/VhdDirectory.js')
const { Disposable } = require('promise-toolbox')
const { asyncEach } = require('@vates/async-each')

const { warn } = createLogger('vhd-lib:createVhdDirectoryFromStream')

const buildVhd = Disposable.wrap(async function* (handler, path, inputStream, { concurrency, compression, isDelta }) {
  const vhd = yield VhdDirectory.create(handler, path, { compression })
  const emptyBlock = Buffer.alloc(2 * 1024 * 1024, 0)
  await asyncEach(
    parseVhdStream(inputStream),
    async function (item) {
      switch (item.type) {
        case 'footer':
          vhd.footer = item.footer
          break
        case 'header':
          vhd.header = item.header
          break
        case 'parentLocator':
          await vhd.writeParentLocator({ ...item, data: item.buffer })
          break
        case 'block':
          // automatically thin blocks of key backup
          // we can't thin block of  delta backup since it can be an empty block whom parent block contains data
          if (isDelta || !emptyBlock.equals(item.buffer)) {
            await vhd.writeEntireBlock(item)
          }
          break
        case 'bat':
          // it exists but  I don't care
          break
        default:
          throw new Error(`unhandled type of block generated by parser : ${item.type} while generating ${path}`)
      }
    },
    {
      concurrency,
    }
  )
  await Promise.all([vhd.writeFooter(), vhd.writeHeader(), vhd.writeBlockAllocationTable()])
  return vhd.streamSize()
})

exports.createVhdDirectoryFromStream = async function createVhdDirectoryFromStream(
  handler,
  path,
  inputStream,
  { validator, concurrency = 16, compression, isDelta } = {}
) {
  try {
    const size = await buildVhd(handler, path, inputStream, { concurrency, compression, isDelta })
    if (validator !== undefined) {
      await validator.call(this, path)
    }
    return size
  } catch (error) {
    // cleanup on error
    await handler.rmtree(path).catch(warn)
    throw error
  }
}
