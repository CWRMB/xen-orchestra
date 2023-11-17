// based on implementation in https://github.com/ForbesLindesay/throat
export class Queue {
  #head // stack to push to, reverse order
  #tail = [] // stack to pop from

  get size() {
    return this.#head.length + this.#tail.length
  }

  constructor(iterable) {
    this.#head = iterable !== undefined ? Array.from(iterable) : []
  }

  at(index) {
    const tail = this.#tail
    const nTail = tail.length
    if (index < nTail) {
      return tail[nTail - index - 1]
    }

    index -= nTail

    const head = this.#head
    const nHead = tail.length
    if (index < nHead) {
      return head[index]
    }
  }

  peek() {
    if (this.size !== 0) {
      const value = this.pop()
      this.#tail.push(value)
      return value
    }
  }

  push(value) {
    this.#head.push(value)
  }

  pop() {
    let tail = this.#tail
    if (tail.length === 0) {
      const head = this.#head
      if (head.length === 0) {
        return
      }
      this.#head = tail
      tail = this.#tail = head.reverse()
    }
    return tail.pop()
  }
}

export default class ColEv {
  #timestamp = Date.now()

  async from(timestamp, { signal } = {}) {}
}
