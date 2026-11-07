/**
 * Maps a public collection facade back to the collection it fronts. A join
 * takes the facade object rather than a collection name so that application
 * code can only join collections its own schema generated.
 */
const facadeCollections = new WeakMap<object, string>();

export function registerFacade(facade: object, collection: string): void {
  facadeCollections.set(facade, collection);
}

export function collectionOfFacade(value: unknown): string {
  const collection =
    typeof value === "object" && value !== null
      ? facadeCollections.get(value)
      : undefined;
  if (collection === undefined)
    throw new TypeError("join target must be a generated collection facade");
  return collection;
}
