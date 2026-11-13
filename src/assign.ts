import { ObjectId } from "mongodb";
import { collections, type Assignment } from "./db.js";

export type AssignResult =
  | { kind: "assigned"; assignment: Assignment }
  | { kind: "judge-busy" } // the judge already holds a QUEUED assignment
  | { kind: "none-available" }; // no project is eligible for this judge right now

/** Retry when another request claims the chosen project between our read and insert. */
const MAX_ASSIGN_ATTEMPTS = 25;

function pickRandom<T>(items: T[]): T {
  return items[Math.floor(Math.random() * items.length)];
}

function isDuplicateKeyError(
  err: unknown
): err is { code: number; keyPattern?: Record<string, unknown> } {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    (err as { code: unknown }).code === 11000
  );
}

/**
 * Assigns `judgeId` their next project. See the README for the rules:
 * never repeat a project for a judge, never take a project another judge has
 * QUEUED, prefer the fewest COMPLETED judgments, one QUEUED assignment per judge.
 *
 * `judgeId` is already validated to exist by the route.
 */
export async function assignNext(judgeId: string): Promise<AssignResult> {
  const { assignments, projects } = collections();

  for (let attempt = 0; attempt < MAX_ASSIGN_ATTEMPTS; attempt++) {
    if (await assignments.findOne({ judgeId, status: "QUEUED" })) {
      return { kind: "judge-busy" };
    }

    const [seenByJudge, queuedAnywhere, completedCounts, allProjects] = await Promise.all([
      assignments.distinct("projectId", { judgeId }),
      assignments.distinct("projectId", { status: "QUEUED" }),
      assignments
        .aggregate<{ _id: string; completed: number }>([
          { $match: { status: "COMPLETED" } },
          { $group: { _id: "$projectId", completed: { $sum: 1 } } },
        ])
        .toArray(),
      projects.find().toArray(),
    ]);

    const unavailable = new Set<string>([...seenByJudge, ...queuedAnywhere]);
    const completedByProject = new Map<string, number>(
      completedCounts.map((count): [string, number] => [count._id, count.completed])
    );

    const candidates = allProjects.filter((project) => !unavailable.has(project._id));
    if (candidates.length === 0) {
      return { kind: "none-available" };
    }

    const fewestCompleted = Math.min(
      ...candidates.map((project) => completedByProject.get(project._id) ?? 0)
    );
    const preferred = candidates.filter(
      (project) => (completedByProject.get(project._id) ?? 0) === fewestCompleted
    );

    const assignment: Assignment = {
      _id: new ObjectId(),
      judgeId,
      projectId: pickRandom(preferred)._id,
      status: "QUEUED",
      createdAt: new Date(),
    };

    try {
      await assignments.insertOne(assignment);
      return { kind: "assigned", assignment };
    } catch (err) {
      if (!isDuplicateKeyError(err)) throw err;
      // A unique index from db.ts rejected a concurrent claim: either this judge
      // just received a project elsewhere, or another judge took this project.
      if (err.keyPattern && "judgeId" in err.keyPattern) {
        return { kind: "judge-busy" };
      }
      // The project was taken; re-read availability and pick again.
    }
  }

  throw new Error(`assignNext: gave up after ${MAX_ASSIGN_ATTEMPTS} attempts for judge ${judgeId}`);
}
