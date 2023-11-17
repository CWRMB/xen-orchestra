import { strict as assert } from 'assert'
import * as CM from 'complex-matcher'
import stubTrue from 'lodash/stubTrue.js'

class Collection {
  #providers = new Map()

  get types() {
    return this.#providers.keys()
  }

  addProvider(provider) {
    const providers = this.#providers

    // FIXME: how to handle dynamic types
    for (const type of provider.types) {
      assert(!providers.has(type), `duplicate provider for ${type}`)

      providers.set(type, provider)
    }

    let active = true
    return function removeProvider() {
      if (active) {
        active = false
        for (const type of provider.types) {
          providers.delete(type)
        }
      }
    }
  }

  #getProvider(type) {
    const provider = this.#providers.get(type)
    assert.notEqual(provider, undefined, `missing provider for ${type}`)
    return provider
  }

  async get(type, id) {
    return this.#getProvider(type).get(type, id)
  }

  watch(type, id, cb) {
    return this.#getProvider(type).watch(type, id, cb)
  }

  watchType(type, cb) {
    return this.#getProvider(type).watchAll(cb)
  }

  watchAll(cb) {
    let unsubscribes = Array.from(this.#providers, provider => provider.watchAll(cb))
    return function unsubscribe() {
      if (unsubscribes !== undefined) {
        for (const unsubscribe of unsubscribes) {
          unsubscribe()
        }
        unsubscribes = undefined
      }
    }
  }
}

export default class UnifiedCollection {
  collection = new Collection()

  #subscriptions = new Map()

  constructor(app) {
    app.addApiMethods({
      collection: {
        get({ type, id }) {
          return this.collection.get(type, id)
        },
        subscribe({ type, id, pattern }) {
          const predicate = pattern !== undefined ? createPredicate(pattern) : stubTrue

          const cb = object => {}

          const { collection } = this
          return this.#registerSubscription(
            id === undefined ? collection.watchType(type, cb) : collection.watch(type, id, cb)
          )
        },
        *unsubscribe() {},
      },
    })
  }

  #registerSubscription(subscription) {
    const subscriptions = this.#subscriptions

    let id
    do {
      id = Math.random().toString(36).slice(2)
    } while (subscriptions.has(id))

    subscriptions.set(id, subscription)
  }
}
