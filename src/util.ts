export const coerceUndefinedToNull = <T>(v: T | undefined): T | null => v === undefined ? null : v;

export const coerceUndefinedOrNullToEmptyArray = <T>(v: Array<T> | undefined | null): Array<T> => v == null ? [] : v;

export const unreachable = (x: never): never => { throw new Error(`Unreachable code reached! The types lied! 😭 Unexpected value: ${x}`) };

export const zip = <T, U>(arr1: T[], arr2: U[]): [T,U][] => {
  const length = Math.min(arr1.length, arr2.length);
  const newArray = Array(length);
  for (let i = 0; i < length; i++) {
    newArray[i] = [arr1[i], arr2[i]];
  }
  return newArray;
};

export const crossProduct = <T, U>(arr1: T[], arr2: U[]): [T,U][] => {
  return arr1.flatMap(a1 => arr2.map(a2 => [a1, a2]) as [T,U][]);
};

// export function omap<K,V,X>(a: Record<K,V>, f: (K,V) => X): Array<X> {
export function omap<V,O>(m: { [x: string]: V; },f: (k: string, v: V) => O) {
  return Object.keys(m).map(k => f(k, m[k]))
}

export function stringToBool(x: string | null | undefined): boolean {
  return (/1|true|t|yes|y/i).test(x || '');
}